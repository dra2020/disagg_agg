# disagg-agg
Code to disaggregate and aggregate data associated with a different geometries (shape sets)

The scripts rely on a strategy of breaking the process into several steps, each of which reads its inputs from files and sends its output
to a file. Logging is sent to a file as well.

disagg_agg.py drives the process and detailed comments are there. It relies on a "prepare" module in which you provide path names to all
of the inputs and outputs you care about, and gives you a place to unzip or do any other preprocessing.

The overall strategy is to build maps (dictionaries) between the smaller (e.g block) and larger (e.g precinct) geometries. If mapping from, say, 2016 precincts to 2010 precincts, we'd build 2 such maps. If a block is split between precincts, we'll assign it to the precinct that has the largest percentage of the block's area. Then we disaggregate properties (fields) from the source geometry into the smaller, using a block population map that you provide to distribute the properties among the blocks. (You might pull voting age population from block census data, for example.) Aggregation uses the other map between geometries. The verification step simple counts the property totals and reports differences between before and after.

I have found it helpful to create a main.py in the parent directory to initiate the process, but that's up to you.

Good luck.