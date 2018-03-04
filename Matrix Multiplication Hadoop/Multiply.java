package edu.uta.cse6331;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

class ElementWritable implements Writable {

	private IntWritable tag;
	private IntWritable index;
	private DoubleWritable value;

	public ElementWritable() {
		tag = new IntWritable(0);
		index = new IntWritable(0);
		value = new DoubleWritable(0);
	}

	public ElementWritable(IntWritable tag, IntWritable index, DoubleWritable value) {
		this.tag = tag;
		this.index = index;
		this.value = value;
	}

	public IntWritable getTag() {
		return tag;
	}

	public void setTag(IntWritable tag) {
		this.tag = tag;
	}

	public IntWritable getIndex() {
		return index;
	}

	public void setIndex(IntWritable index) {
		this.index = index;
	}

	public DoubleWritable getValue() {
		return value;
	}

	public void setValue(DoubleWritable value) {
		this.value = value;
	}

	public void readFields(DataInput input) throws IOException {

		tag.readFields(input);
		index.readFields(input);
		value.readFields(input);

	}

	public void write(DataOutput output) throws IOException {

		tag.write(output);
		index.write(output);
		value.write(output);
	}

	public String toString() {
		return tag.get() + " " + index.get() + " " + value.get();
	}

}

class PairWritable implements Writable, WritableComparable<PairWritable> {

	private IntWritable rowIndex;
	private IntWritable columnIndex;

	public PairWritable() {

		rowIndex = new IntWritable(0);
		columnIndex = new IntWritable(0);

	}

	public IntWritable getRowIndex() {
		return rowIndex;
	}

	public void setRowIndex(IntWritable rowIndex) {
		this.rowIndex = rowIndex;
	}

	public IntWritable getColumnIndex() {
		return columnIndex;
	}

	public void setColumnIndex(IntWritable columnIndex) {
		this.columnIndex = columnIndex;
	}

	public void readFields(DataInput input) throws IOException {

		rowIndex.readFields(input);
		columnIndex.readFields(input);

	}

	public void write(DataOutput output) throws IOException {

		rowIndex.write(output);
		columnIndex.write(output);

	}

	@Override
	public String toString() {
		return rowIndex.get() + " " + columnIndex.get();
	}

	public int compareTo(PairWritable pairWritable) {
		if (rowIndex.get() > pairWritable.getRowIndex().get()) {
			return 1;
		} else if (rowIndex.get() < pairWritable.getRowIndex().get()) {
			return -1;
		} else if (rowIndex.get() == pairWritable.getRowIndex().get()) {
			if (columnIndex.get() > pairWritable.getColumnIndex().get()) {
				return 1;
			} else if (columnIndex.get() < pairWritable.getColumnIndex().get()) {
				return -1;
			}
		}
		return 0;
	}

}

public class Multiply {

	public static class MapperM extends Mapper<Object, Text, IntWritable, ElementWritable> {

		private IntWritable currentTag = new IntWritable(0);

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			StringTokenizer mainStringTokens = new StringTokenizer(value.toString(), ",");

			while (mainStringTokens.hasMoreTokens()) {

				IntWritable valueIndex = new IntWritable(Integer.parseInt(mainStringTokens.nextToken()));
				IntWritable keyIndex = new IntWritable(Integer.parseInt(mainStringTokens.nextToken()));
				DoubleWritable actualValue = new DoubleWritable(Double.parseDouble(mainStringTokens.nextToken()));

				ElementWritable elementWritable = new ElementWritable();
				elementWritable.setTag(currentTag);
				elementWritable.setIndex(valueIndex);
				elementWritable.setValue(actualValue);

				System.out.println("-------------" + elementWritable);

				context.write(keyIndex, elementWritable);
			}

		}

	}

	public static class MapperN extends Mapper<Object, Text, IntWritable, ElementWritable> {

		private IntWritable currentTag = new IntWritable(1);

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			StringTokenizer mainStringTokens = new StringTokenizer(value.toString(), ",");

			while (mainStringTokens.hasMoreTokens()) {

				IntWritable keyIndex = new IntWritable(Integer.parseInt(mainStringTokens.nextToken()));
				IntWritable valueIndex = new IntWritable(Integer.parseInt(mainStringTokens.nextToken()));
				DoubleWritable actualValue = new DoubleWritable(Double.parseDouble(mainStringTokens.nextToken()));

				ElementWritable elementWritable = new ElementWritable();
				elementWritable.setTag(currentTag);
				elementWritable.setIndex(valueIndex);
				elementWritable.setValue(actualValue);

				context.write(keyIndex, elementWritable);

			}

		}

	}

	public static class ProductReducer extends Reducer<IntWritable, ElementWritable, PairWritable, DoubleWritable> {

		@Override
		public void reduce(IntWritable key, Iterable<ElementWritable> values, Context context)
				throws IOException, InterruptedException {

			ArrayList<ElementWritable> listMValues = new ArrayList<ElementWritable>();
			ArrayList<ElementWritable> listNValues = new ArrayList<ElementWritable>();

			Configuration conf = context.getConfiguration();

			for (ElementWritable value : values) {

				ElementWritable tempElementWritable = ReflectionUtils.newInstance(ElementWritable.class, conf);
				ReflectionUtils.copy(conf, value, tempElementWritable);

				if (tempElementWritable.getTag().get() == 0) {
					listMValues.add(tempElementWritable);
				} else {
					listNValues.add(tempElementWritable);
				}
			}

			for (ElementWritable matrixMValue : listMValues) {

				for (ElementWritable matrixNValue : listNValues) {

					PairWritable pairWritable = new PairWritable();

					pairWritable.setRowIndex(matrixMValue.getIndex());
					pairWritable.setColumnIndex(matrixNValue.getIndex());

					DoubleWritable productValue = new DoubleWritable(
							matrixMValue.getValue().get() * matrixNValue.getValue().get());

					context.write(pairWritable, productValue);

				}
			}

		}

	}

	public static class IdentityMapper extends Mapper<Object, Text, PairWritable, DoubleWritable> {

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			StringTokenizer stringTokenize = new StringTokenizer(value.toString());

			while (stringTokenize.hasMoreTokens()) {

				PairWritable pairWritable = new PairWritable();
				pairWritable.setRowIndex(new IntWritable(Integer.parseInt(stringTokenize.nextToken())));
				pairWritable.setColumnIndex(new IntWritable(Integer.parseInt(stringTokenize.nextToken())));

				DoubleWritable productIntWritable = new DoubleWritable(Double.parseDouble(stringTokenize.nextToken()));

				context.write(pairWritable, productIntWritable);

			}

		}
	}

	public static class AggregateReducer extends Reducer<PairWritable, DoubleWritable, PairWritable, DoubleWritable> {

		private DoubleWritable totalProductSum = new DoubleWritable();

		@Override
		public void reduce(PairWritable key, Iterable<DoubleWritable> values, Context context)
				throws IOException, InterruptedException {

			double productSum = 0.0;

			for (DoubleWritable value : values) {
				productSum = productSum + value.get();
			}

			totalProductSum.set(productSum);
			context.write(key, totalProductSum);

		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration configuration = new Configuration();

		Job multiplyJob = Job.getInstance(configuration, "Matrix Multiply");
		multiplyJob.setJarByClass(Multiply.class);
		MultipleInputs.addInputPath(multiplyJob, new Path(args[0]), TextInputFormat.class, MapperM.class);
		MultipleInputs.addInputPath(multiplyJob, new Path(args[1]), TextInputFormat.class, MapperN.class);
		multiplyJob.setMapOutputKeyClass(IntWritable.class);
		multiplyJob.setMapOutputValueClass(ElementWritable.class);
		multiplyJob.setReducerClass(ProductReducer.class);
		multiplyJob.setOutputKeyClass(Text.class);
		multiplyJob.setOutputValueClass(IntWritable.class);
		FileOutputFormat.setOutputPath(multiplyJob, new Path(args[2]));
		multiplyJob.waitForCompletion(true);

		Job aggregateJob = Job.getInstance(configuration, "Matrix Aggregate");
		aggregateJob.setJarByClass(Multiply.class);
		aggregateJob.setMapperClass(IdentityMapper.class);
		aggregateJob.setMapOutputKeyClass(PairWritable.class);
		aggregateJob.setMapOutputValueClass(DoubleWritable.class);
		aggregateJob.setReducerClass(AggregateReducer.class);
		aggregateJob.setOutputKeyClass(PairWritable.class);
		aggregateJob.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(aggregateJob, new Path(args[2]));
		FileOutputFormat.setOutputPath(aggregateJob, new Path(args[3]));
		aggregateJob.waitForCompletion(true);
	}

}
