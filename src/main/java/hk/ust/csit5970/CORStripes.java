package hk.ust.csit5970;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;
import java.util.Map.Entry;
/**
 * Compute the bigram count using "pairs" approach
 */
public class CORStripes extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(CORStripes.class);
	private final static IntWritable ONE = new IntWritable(1);
    private static Text word = new Text();
	/*
	 * TODO: Write your first-pass Mapper here.
	 */
	private static class CORMapper1 extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			HashMap<String, Integer> word_set = new HashMap<String, Integer>();
			// Please use this tokenizer! DO NOT implement a tokenizer by yourself!
			String clean_doc = value.toString().replaceAll("[^a-z A-Z]", " ");
			StringTokenizer doc_tokenizer = new StringTokenizer(clean_doc);
			/*
			 * TODO: Your implementation goes here.
			 */
			while (doc_tokenizer.hasMoreTokens()) {
            	String token = doc_tokenizer.nextToken();
            	Integer currentCount = word_set.get(token); // 先尝试获取当前值
				if (currentCount == null) {
					currentCount = 0; // 如果不存在，默认 0
				}
				word_set.put(token, currentCount + 1); // 更新计数
        	}
	        for (Map.Entry<String, Integer> entry : word_set.entrySet()) {
            	word.set(entry.getKey());
            	context.write(word, new IntWritable(entry.getValue()));
        	}
		}
	}

	/*
	 * TODO: Write your first-pass reducer here.
	 */
	private static class CORReducer1 extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        	int sum = 0; 
        	for (IntWritable val : values) {
            	sum += val.get(); 
        	}
        	result.set(sum); 
        	context.write(key, result); 
    	}
	}

	/*
	 * TODO: Write your second-pass Mapper here.
	 */
	public static class CORStripesMapper2 extends Mapper<LongWritable, Text, Text, MapWritable> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Set<String> sorted_word_set = new TreeSet<String>();
			// Please use this tokenizer! DO NOT implement a tokenizer by yourself!
			String doc_clean = value.toString().replaceAll("[^a-z A-Z]", " ");
			StringTokenizer doc_tokenizers = new StringTokenizer(doc_clean);
			while (doc_tokenizers.hasMoreTokens()) {
				sorted_word_set.add(doc_tokenizers.nextToken());
			}
			/*
			 * TODO: Your implementation goes here.
			 */
			// Step 2: 生成所有唯一的标准化单词对
			List<String> words = new ArrayList<String>(sorted_word_set);
			for (int i = 0; i < words.size(); i++) {
				String word = words.get(i);
				MapWritable stripe = new MapWritable();  // 存储当前单词的邻居计数
				
				for (int j = 0; j < words.size(); j++) {
					if (i == j) continue;  // 跳过自身组合
					
					String neighbor = words.get(j);
					// 标准化：保证字母顺序小的单词作为主词
					if (word.compareTo(neighbor) < 0) {
						// 只在主词小于邻居时记录，避免重复计数
						Text neighborText = new Text(neighbor);
						IntWritable count = (IntWritable) stripe.get(neighborText);
						if (count == null) {
							stripe.put(neighborText, new IntWritable(1));
						} else {
							count.set(count.get() + 1);
						}
					}
				}
				
				// 发射格式: (word, {"neighbor1":1, "neighbor2":1, ...})
				if (!stripe.isEmpty()) {
					context.write(new Text(word), stripe);
				}
			}
		}
	}

	/*
	 * TODO: Write your second-pass Combiner here.
	 */
	public static class CORStripesCombiner2 extends Reducer<Text, MapWritable, Text, MapWritable> {
		private static final IntWritable ONE = new IntWritable(1);

		@Override
		protected void reduce(Text key, Iterable<MapWritable> values, Context context) 
				throws IOException, InterruptedException {
			
			// 初始化合并后的 stripe
			MapWritable mergedStripe = new MapWritable();

			// 遍历所有输入的 stripes
			for (MapWritable stripe : values) {
				// 遍历当前 stripe 的所有条目
				for (Entry<Writable, Writable> entry : stripe.entrySet()) {
					Text neighbor = (Text) entry.getKey();
					IntWritable count = (IntWritable) entry.getValue();
					
					// 检查是否已经存在于合并后的 stripe 中
					IntWritable existingCount = (IntWritable) mergedStripe.get(neighbor);
					if (existingCount == null) {
						// 如果不存在，添加新的条目
						mergedStripe.put(neighbor, new IntWritable(count.get()));
					} else {
						// 如果已存在，累加计数
						existingCount.set(existingCount.get() + count.get());
					}
				}
			}

			// 发射合并后的结果
			context.write(key, mergedStripe);
		}
	}

	/*
	 * TODO: Write your second-pass Reducer here.
	 */
public static class CORStripesReducer2 extends Reducer<Text, MapWritable, PairOfStrings, DoubleWritable> {
    private static Map<String, Integer> word_total_map = new HashMap<String, Integer>();
    private static final IntWritable ZERO = new IntWritable(0);
    private DoubleWritable result = new DoubleWritable();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Path middle_result_path = new Path("mid/part-r-00000");
		Configuration middle_conf = new Configuration();
		try {
			FileSystem fs = FileSystem.get(URI.create(middle_result_path.toString()), middle_conf);

			if (!fs.exists(middle_result_path)) {
				throw new IOException(middle_result_path.toString() + "not exist!");
			}

			FSDataInputStream in = fs.open(middle_result_path);
			InputStreamReader inStream = new InputStreamReader(in);
			BufferedReader reader = new BufferedReader(inStream);

			LOG.info("reading...");
			String line = reader.readLine();
			String[] line_terms;
			while (line != null) {
				line_terms = line.split("\t");
				word_total_map.put(line_terms[0], Integer.valueOf(line_terms[1]));
				LOG.info("read one line!");
				line = reader.readLine();
			}
			reader.close();
			LOG.info("finished！");
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}
	
    @Override
    protected void reduce(Text key, Iterable<MapWritable> values, Context context)
            throws IOException, InterruptedException {
        
        // Step 1: 获取当前单词A的总频次 Freq(A) (Java 1.5兼容写法)
        String wordA = key.toString();
        Integer freqAObj = word_total_map.get(wordA);
        int freqA = (freqAObj != null) ? freqAObj : 0;
        if (freqA == 0) return;

        // Step 2: 合并所有stripes (Java 1.5兼容写法)
        Map<String, Integer> neighborCounts = new HashMap<String, Integer>();
        for (MapWritable stripe : values) {
            for (Map.Entry<Writable, Writable> entry : stripe.entrySet()) {
                String wordB = ((Text) entry.getKey()).toString();
                int count = ((IntWritable) entry.getValue()).get();
                
                Integer currentCountObj = neighborCounts.get(wordB);
                int currentCount = (currentCountObj != null) ? currentCountObj : 0;
                neighborCounts.put(wordB, currentCount + count);
            }
        }

        // Step 3: 计算并输出COR(A,B)
        for (Map.Entry<String, Integer> entry : neighborCounts.entrySet()) {
            String wordB = entry.getKey();
            int freqAB = entry.getValue();
            
            // Java 1.5兼容的null检查
            Integer freqBObj = word_total_map.get(wordB);
            int freqB = (freqBObj != null) ? freqBObj : 0;
            
            if (freqB > 0) {
                // 计算COR(A,B)
                double cor = (double)freqAB / (freqA * freqB);
                result.set(cor);
                
                // 创建标准化单词对（字典序）
                PairOfStrings pair;
                if (wordA.compareTo(wordB) < 0) {
                    pair = new PairOfStrings(wordA, wordB);
                } else {
                    pair = new PairOfStrings(wordB, wordA);
                }
                
                context.write(pair, result);
            }
        }
    }
}

	/**
	 * Creates an instance of this tool.
	 */
	public CORStripes() {
	}

	private static final String INPUT = "input";
	private static final String OUTPUT = "output";
	private static final String NUM_REDUCERS = "numReducers";

	/**
	 * Runs this tool.
	 */
	@SuppressWarnings({ "static-access" })
	public int run(String[] args) throws Exception {
		Options options = new Options();

		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("input path").create(INPUT));
		options.addOption(OptionBuilder.withArgName("path").hasArg()
				.withDescription("output path").create(OUTPUT));
		options.addOption(OptionBuilder.withArgName("num").hasArg()
				.withDescription("number of reducers").create(NUM_REDUCERS));

		CommandLine cmdline;
		CommandLineParser parser = new GnuParser();

		try {
			cmdline = parser.parse(options, args);
		} catch (ParseException exp) {
			System.err.println("Error parsing command line: "
					+ exp.getMessage());
			return -1;
		}

		// Lack of arguments
		if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
			System.out.println("args: " + Arrays.toString(args));
			HelpFormatter formatter = new HelpFormatter();
			formatter.setWidth(120);
			formatter.printHelp(this.getClass().getName(), options);
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}

		String inputPath = cmdline.getOptionValue(INPUT);
		String middlePath = "mid";
		String outputPath = cmdline.getOptionValue(OUTPUT);

		int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? Integer
				.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;

		LOG.info("Tool: " + CORStripes.class.getSimpleName());
		LOG.info(" - input path: " + inputPath);
		LOG.info(" - middle path: " + middlePath);
		LOG.info(" - output path: " + outputPath);
		LOG.info(" - number of reducers: " + reduceTasks);

		// Setup for the first-pass MapReduce
		Configuration conf1 = new Configuration();

		Job job1 = Job.getInstance(conf1, "Firstpass");

		job1.setJarByClass(CORStripes.class);
		job1.setMapperClass(CORMapper1.class);
		job1.setReducerClass(CORReducer1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job1, new Path(inputPath));
		FileOutputFormat.setOutputPath(job1, new Path(middlePath));

		// Delete the output directory if it exists already.
		Path middleDir = new Path(middlePath);
		FileSystem.get(conf1).delete(middleDir, true);

		// Time the program
		long startTime = System.currentTimeMillis();
		job1.waitForCompletion(true);
		LOG.info("Job 1 Finished in " + (System.currentTimeMillis() - startTime)
				/ 1000.0 + " seconds");

		// Setup for the second-pass MapReduce

		// Delete the output directory if it exists already.
		Path outputDir = new Path(outputPath);
		FileSystem.get(conf1).delete(outputDir, true);


		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "Secondpass");

		job2.setJarByClass(CORStripes.class);
		job2.setMapperClass(CORStripesMapper2.class);
		job2.setCombinerClass(CORStripesCombiner2.class);
		job2.setReducerClass(CORStripesReducer2.class);

		job2.setOutputKeyClass(PairOfStrings.class);
		job2.setOutputValueClass(DoubleWritable.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(MapWritable.class);
		job2.setNumReduceTasks(reduceTasks);

		FileInputFormat.setInputPaths(job2, new Path(inputPath));
		FileOutputFormat.setOutputPath(job2, new Path(outputPath));

		// Time the program
		startTime = System.currentTimeMillis();
		job2.waitForCompletion(true);
		LOG.info("Job 2 Finished in " + (System.currentTimeMillis() - startTime)
				/ 1000.0 + " seconds");

		return 0;
	}

	/**
	 * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new CORStripes(), args);
	}
}