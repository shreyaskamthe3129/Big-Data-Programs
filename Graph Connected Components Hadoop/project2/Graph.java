package edu.uta.cse6331;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

class Vertex implements Writable {

	private int graphTag;
	private long graphGroup;
	private long vertexId;
	private ArrayList<Long> listOfAdjacent;

	public int getGraphTag() {
		return graphTag;
	}

	public void setGraphTag(int graphTag) {
		this.graphTag = graphTag;
	}

	public long getGraphGroup() {
		return graphGroup;
	}

	public void setGraphGroup(long graphGroup) {
		this.graphGroup = graphGroup;
	}

	public long getVertexId() {
		return vertexId;
	}

	public void setVertexId(long vertexId) {
		this.vertexId = vertexId;
	}

	public ArrayList<Long> getListOfAdjacent() {
		return listOfAdjacent;
	}

	public void setListOfAdjacent(ArrayList<Long> listOfAdjacent) {
		this.listOfAdjacent = listOfAdjacent;
	}

	public Vertex() {
		
	}

	public Vertex(int graphTag, long graphGroup, long vertexId, ArrayList<Long> listOfAdjacent) {

		this.graphTag = graphTag;
		this.graphGroup = graphGroup;
		this.vertexId = vertexId;
		this.listOfAdjacent = listOfAdjacent;

	}

	public Vertex(int graphTag, long graphGroup) {

		this.graphTag = graphTag;
		this.graphGroup = graphGroup;
		//this.vertexId = 0;
		this.listOfAdjacent = new ArrayList<Long>();

	}

	public void readFields(DataInput input) throws IOException {
		// listOfAdjacent.clear();

		this.listOfAdjacent = new ArrayList<Long>();
		this.graphTag = input.readInt();
	 	this.graphGroup = input.readLong();
	 	this.vertexId = input.readLong();
		int listSize = input.readInt();
		for (int count = 0; count < listSize; count++) {
			long sizeItem = input.readLong();
			listOfAdjacent.add(sizeItem);
		}

	}

	public void write(DataOutput output) throws IOException {
		output.writeInt(graphTag);
		output.writeLong(graphGroup);
		output.writeLong(vertexId);
		output.writeInt(listOfAdjacent.size());
		for (long vertexVal : listOfAdjacent) {
			output.writeLong(vertexVal);
		}
	}

	/*
	 * @Override public String toString() { return graphTag.get() + " " +
	 * graphGroup.get() + " " + vertexId.get() + " " +
	 * listOfAdjacent.toString(); }
	 */
}

public class Graph {

	public static class TagMapper extends Mapper<Object, Text, LongWritable, Vertex> {

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			StringTokenizer stringTokens = new StringTokenizer(value.toString(), ",");
			int tokenCount = stringTokens.countTokens();
			while (stringTokens.hasMoreTokens()) {
				long vertexId = Long.parseLong(stringTokens.nextToken());
				ArrayList<Long> listOfAdjacent = new ArrayList<Long>();
				for (int counter = 1; counter < tokenCount; counter++) {
					listOfAdjacent.add(Long.parseLong(stringTokens.nextToken()));
				}
				Vertex graphVertex = new Vertex(0, vertexId, vertexId, listOfAdjacent);
				context.write(new LongWritable(vertexId), graphVertex);
			}

		}

	}

	public static class TagReducer extends Reducer<LongWritable, Vertex, LongWritable, Vertex> {

		@Override
		public void reduce(LongWritable key, Iterable<Vertex> values, Context context)
				throws IOException, InterruptedException {

			for (Vertex currentVertex : values) {
				context.write(new LongWritable(currentVertex.getVertexId()), currentVertex);
			}

		}

	}

	public static class GroupMapper extends Mapper<LongWritable, Vertex, LongWritable, Vertex> {

		@Override
		public void map(LongWritable key, Vertex value, Context context) throws IOException, InterruptedException {

			Vertex currentVertex = value;
			context.write(new LongWritable(currentVertex.getVertexId()), currentVertex);
			for (Long vid : currentVertex.getListOfAdjacent()) {
				context.write(new LongWritable(vid), new Vertex(1, currentVertex.getGraphGroup()));
			}
		}

	}

	public static class GroupReducer extends Reducer<LongWritable, Vertex, LongWritable, Vertex> {

		@Override
		public void reduce(LongWritable key, Iterable<Vertex> values, Context context)
				throws IOException, InterruptedException {

			long currentGroupValue = Long.MAX_VALUE;
			long currentVertexId = key.get();
			ArrayList<Long> tempAdjacentList = new ArrayList<Long>();

			for (Vertex currentVertex : values) {

				if (currentVertex.getGraphTag() == 0) {
					tempAdjacentList = currentVertex.getListOfAdjacent();
				}
				currentGroupValue = Math.min(currentGroupValue, currentVertex.getGraphGroup());
			}
			context.write(new LongWritable(currentGroupValue),
					new Vertex(0, currentGroupValue, currentVertexId, tempAdjacentList));
		}

	}

	public static class AggregateMapper extends Mapper<LongWritable, Vertex, LongWritable, IntWritable> {

		@Override
		public void map(LongWritable key, Vertex value, Context context) throws IOException, InterruptedException {

			context.write(new LongWritable(value.getGraphGroup()), new IntWritable(1));

		}

	}

	public static class AggregateReducer extends Reducer<LongWritable, IntWritable, LongWritable, IntWritable> {

		@Override
		public void reduce(LongWritable key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			int totalCount = 0;

			for (IntWritable value : values) {
				totalCount = totalCount + value.get();
			}
			context.write(key, new IntWritable(totalCount));
		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration configuration = new Configuration();

		Job vertexGraphJob = Job.getInstance(configuration, "Vertex Graph");
		vertexGraphJob.setJarByClass(Graph.class);
		vertexGraphJob.setInputFormatClass(TextInputFormat.class);
		vertexGraphJob.setOutputFormatClass(SequenceFileOutputFormat.class);

		vertexGraphJob.setMapperClass(TagMapper.class);
		vertexGraphJob.setMapOutputKeyClass(LongWritable.class);
		vertexGraphJob.setMapOutputValueClass(Vertex.class);

		vertexGraphJob.setReducerClass(TagReducer.class);
		vertexGraphJob.setOutputKeyClass(LongWritable.class);
		vertexGraphJob.setOutputValueClass(Vertex.class);

		FileInputFormat.addInputPath(vertexGraphJob, new Path(args[0]));
		SequenceFileOutputFormat.setOutputPath(vertexGraphJob, new Path(args[1] + "/folder0"));
		vertexGraphJob.waitForCompletion(true);

		for (int jobCount = 0; jobCount < 5; jobCount++) {

			Job vertexGroupJob = Job.getInstance(configuration, "Vertex Group");

			vertexGroupJob.setJarByClass(Graph.class);
			vertexGroupJob.setInputFormatClass(SequenceFileInputFormat.class);
			vertexGroupJob.setOutputFormatClass(SequenceFileOutputFormat.class);

			vertexGroupJob.setMapperClass(GroupMapper.class);
			vertexGroupJob.setMapOutputKeyClass(LongWritable.class);
			vertexGroupJob.setMapOutputValueClass(Vertex.class);

			vertexGroupJob.setReducerClass(GroupReducer.class);
			vertexGroupJob.setOutputKeyClass(LongWritable.class);
			vertexGroupJob.setOutputValueClass(Vertex.class);

			SequenceFileInputFormat.addInputPath(vertexGroupJob, new Path(args[1] + "/folder" + jobCount));
			SequenceFileOutputFormat.setOutputPath(vertexGroupJob, new Path(args[1] + "/folder" + (jobCount + 1)));
			vertexGroupJob.waitForCompletion(true);

		}

		Job aggregateJob = Job.getInstance(configuration, "Aggregate Vertex");
		aggregateJob.setJarByClass(Graph.class);
		aggregateJob.setInputFormatClass(SequenceFileInputFormat.class);

		aggregateJob.setMapperClass(AggregateMapper.class);
		aggregateJob.setMapOutputKeyClass(LongWritable.class);
		aggregateJob.setMapOutputValueClass(IntWritable.class);

		aggregateJob.setReducerClass(AggregateReducer.class);
		aggregateJob.setOutputKeyClass(LongWritable.class);
		aggregateJob.setOutputValueClass(IntWritable.class);

		SequenceFileInputFormat.addInputPath(aggregateJob, new Path(args[1] + "/folder5"));
		FileOutputFormat.setOutputPath(aggregateJob, new Path(args[2]));
		aggregateJob.waitForCompletion(true);

	}
}