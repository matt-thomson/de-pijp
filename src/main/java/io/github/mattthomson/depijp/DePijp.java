package io.github.mattthomson.depijp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DePijp extends Configured implements Tool {
    private static final Logger logger = LoggerFactory.getLogger(DePijp.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new DePijp(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        try {
            String flowClassName = args[0];
            DePijpFlow flow = (DePijpFlow) Class.forName(flowClassName).newInstance();

            PijpBuilder builder = new PijpBuilder();
            flow.flow(builder, args);

            builder.run();
            return 0;
        } catch (Exception e) {
            logger.error("Error running flow", e);
            return 1;
        }
    }
}
