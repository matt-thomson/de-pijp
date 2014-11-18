package io.github.mattthomson.depijp;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

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
            boolean isLocal = isLocal(args);

            PijpBuilder builder = getPijpBuilder(isLocal);
            flow.flow(builder, getArgs(args, isLocal));

            builder.run();
            return 0;
        } catch (Exception e) {
            logger.error("Error running flow", e);
            return 1;
        }
    }

    private boolean isLocal(String[] args) {
        return args.length > 1 && StringUtils.equals("--local", args[1]);
    }

    private PijpBuilder getPijpBuilder(boolean isLocal) {
        if (isLocal) {
            return PijpBuilder.local();
        } else {
            return PijpBuilder.hadoop();
        }
    }

    private String[] getArgs(String[] args, boolean isLocal) {
        int numToSkip = isLocal ? 2 : 1;
        return Arrays.copyOfRange(args, numToSkip, args.length);
    }
}
