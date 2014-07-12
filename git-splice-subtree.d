/// Merges the mainline history of several D repositories
/// by rearranging each mainline commit's tree (or a subtree)
/// as a subtree in the created repo.
/// Spec file passed on the command line has one repo per line,
/// with 2-3 tab-separated fields per line: target subtree,
/// source repository URL, and optionally the source subtree.
module git_splice_subtree;

import std.algorithm;
import std.array;
import std.conv;
import std.exception;
import std.file;
import std.path;
import std.process;
import std.range;
import std.regex;
import std.stdio;
import std.string;
import std.parallelism : parallel;

import ae.sys.cmd;
import ae.sys.file;
import ae.sys.git;
import ae.utils.aa;

// http://d.puremagic.com/issues/show_bug.cgi?id=7016
version(Windows) static import ae.sys.windows;

void main(string[] args)
{
	enforce(args.length == 2, "Incorrect number of command-line parameters
Usage: " ~ args[0] ~ " SPECFILE
SPECFILE lines are: TARGET-SUBTREE SOURCE-URL [SOURCE-SUBTREE]");

	static struct Source
	{
		string name, url;
		string[] sourceTree, targetTree;
	}
	Source[] sources;

	auto branchName = "master";

	foreach (line; File(args[1]).byLine().map!(s => s.idup.strip()))
	{
		auto fields = line.split("\t").filter!(s => s.length).array();
		if (fields.length == 0)
			continue;
		enforce(fields.length >= 2 && fields.length <= 3, "Bad line: " ~ line);
		auto url = fields[1];
		auto name = repositoryNameFromURL(url);
		string sourceTree;
		if (fields.length > 2)
			sourceTree = fields[2];
		sources ~= Source(name, url, sourceTree.splitTreePath(), fields[0].splitTreePath());
	}
	//stderr.writeln(sources);

	auto resultDir = "result";

	if (!resultDir.exists)
	{
		mkdir(resultDir);
		auto repo = Repository(resultDir);
		repo.run("init");
	}

	auto repo = new Repository(resultDir);

	debug {} else
	{
		stderr.writeln("Fetching...");
		foreach (ref source; sources)
			//repo.run("fetch", source.url, "+refs/*:refs/sources/%s/*".format(source.name));
			repo.run("fetch", source.url, "+refs/heads/%s:refs/sources/%s/heads/%s".format(branchName, source.name, branchName));
	}

	stderr.writeln("Loading history...");
	History history = repo.getHistory();

	stderr.writeln("Examining history...");

	auto reReverseMerge = regex(`^Merge branch 'master' of github`);

	static struct Merge
	{
		Source* source;
		Commit* commit;
	}
	Merge[][string] repoMerges;
	Commit*[string] tags;

	uint latestTime;

	foreach (ref source; sources)
	{
		auto refName = "refs/sources/%s/heads/%s".format(source.name, branchName);
		auto refHash = history.refs[refName];

		auto time = history.commits[refHash].time;
		if (time > latestTime)
			latestTime = time;

		Merge[] merges;
		Commit* c = history.commits[refHash];
		do
		{
			merges ~= Merge(&source, c);
			if (c.message.length && c.message[0].match(reReverseMerge))
			{
				enforce(c.parents.length == 2);
				c = c.parents[1];
			}
			else
				c = c.parents.length ? c.parents[0] : null;
		} while (c);
		repoMerges[source.name] = merges;
		//writefln("%d linear history commits in %s", linearHistory.length, repoName);
	}

	auto allMerges = repoMerges.values.join;
	allMerges.sort!(`a.commit.time > b.commit.time`, SwapStrategy.stable)();
	allMerges.reverse;
	auto end = allMerges.countUntil!(m => m.commit.time > latestTime);
	if (end >= 0)
		allMerges = allMerges[0..end];

	stderr.writeln("Loading commits...");
	auto commitObjects = repo.getObjects(allMerges.map!(m => m.commit.hash).array());
	auto treeHashes = commitObjects.map!(o => o.parseCommit().tree).array();
	assert(treeHashes.length == allMerges.length);

	// Traverse the source trees, as necesssary
	auto maxSourceTreeDepth = sources.map!(s => s.sourceTree.length).reduce!max();
	foreach (depth; 0..maxSourceTreeDepth)
	{
		stderr.writefln("Traversing commit trees, level %d...", depth);
		auto indicesToTraverse =
			treeHashes.length.iota
			.filter!(n => depth < allMerges[n].source.sourceTree.length && treeHashes[n] != Hash.init)
			.array();

		auto traversedTrees = repo.getObjects(indicesToTraverse.map!(n => treeHashes[n]).array());

		auto traversedHashes =
			indicesToTraverse.length.iota
			.map!(
				(n)
				{
					auto dirName = allMerges[indicesToTraverse[n]].source.sourceTree[depth];
					auto dirEntry = traversedTrees[n]
						.parseTree()
						.filter!(e => e.name == dirName);
					return dirEntry.empty
						? Hash.init
						: dirEntry.front.hash;
				}
			)
			.array()
		;

		foreach (i, n; indicesToTraverse)
			treeHashes[n] = traversedHashes[i];
	}

	stderr.writeln("Computing tree objects...");
	Hash[string] currentTree;
	auto resultTrees =
		allMerges.length.iota
		.map!(
			(n)
			{
				auto targetTree = allMerges[n].source.targetTree;
				enforce(targetTree.length == 1, "TODO: non-root target");
				if (treeHashes[n] != Hash.init)
					currentTree[targetTree[0]] = treeHashes[n];
				return GitObject.createTree(
					currentTree
					.pairs
					.sort!((a, b) => a.key < b.key)
					.map!(pair => GitObject.TreeEntry(octal!40000, pair.key, pair.value))
					.array());
			}
		)
		.array();

	stderr.writeln("Writing tree objects...");
	repo.writeObjects(resultTrees);

	stderr.writeln("Writing commit objects...");
	Hash[] parents;
	{
		auto writer = repo.createObjectWriter("commit");
		foreach (n, ref obj; commitObjects)
		{
			if (n && resultTrees[n].hash == resultTrees[n-1].hash)
				continue; // Skip commits with changes outside chosen source tree
			auto commit = obj.parseCommit();
			commit.tree = resultTrees[n].hash;
			commit.parents = parents;
			if (commit.message.length)
				commit.message[0] = allMerges[n].source.name ~ ": " ~ commit.message[0];
			parents = [writer.write(GitObject.createCommit(commit).data)];
		}
	}

	stderr.writeln("Creating branch....");
	//repo.run("branch", "-f", branchName, parents[0].toString());
	repo.run("update-ref", "-m", "git-splice-subtree reset", "refs/heads/" ~ branchName, parents[0].toString());
	repo.run("reset", "--hard", "master");

	stderr.writeln("Done.");
}

string[] splitTreePath(string tree) { return tree.splitter('/').filter!(s => s.length).array(); }
