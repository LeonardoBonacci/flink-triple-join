package guru.bonacci.join;

import static org.apache.flink.table.api.Expressions.$;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TheTripleJoinJob {

	
	public static void main(String[] args) throws Exception {
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

		DataStream<Tuple2<Integer, String>> stream1 = env.fromElements(new Tuple2<>(1, "hello"));
		DataStream<Tuple2<Integer, String>> stream2 = env.fromElements(new Tuple2<>(1, "hello2"));
		DataStream<Tuple3<Integer, String, String>> stream3 = env.fromElements(new Tuple3<>(1, "hello3", "bye3"));

		Table table1 = tEnv.fromDataStream(stream1, $("id1"), $("word1"));
		Table table2 = tEnv.fromDataStream(stream2, $("id2"), $("word2"));
		Table table3 = tEnv.fromDataStream(stream3, $("id3"), $("word3"), $("anotherword3"));

		Table table12 = table1.join(table2)
		    .where($("id1").isEqual($("id2")))
		    .select($("id1"), $("word1"), $("word2"));

		Table table123 = table12.join(table3)
		    .where($("id1").isEqual($("id3")))
		    .select($("id1"), $("word1"), $("word2"), $("word3"), $("anotherword3"));

//		table123.execute().print();
		
		DataStream<Row> resultStream = tEnv.toDataStream(table123);

		resultStream.print();
		env.execute();
	}
}