package cn.edu.thu.kvtsfile.spark;


import cn.edu.thu.kvtsfile.spark.common.BasicOperator;
import cn.edu.thu.kvtsfile.spark.common.FilterOperator;
import cn.edu.thu.kvtsfile.spark.common.SQLConstant;
import cn.edu.thu.kvtsfile.spark.common.TSQueryPlan;
import cn.edu.thu.kvtsfile.spark.exception.QueryProcessorException;
import cn.edu.thu.tsfile.timeseries.read.LocalFileInput;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * @author QJL
 */
public class QueryPlanTest {

    private String tsfilePath = "src/test/resources/test.tsfile";

    @Before
    public void before() throws Exception {
        new CreateKmxTSFile().createTSFile1(tsfilePath);
    }

    @Test
    public void testQp() throws IOException, QueryProcessorException {
        LocalFileInput in = new LocalFileInput(tsfilePath);
        FilterOperator filterOperator = new FilterOperator(SQLConstant.KW_AND);
        filterOperator.addChildOPerator(new BasicOperator(SQLConstant.GREATERTHAN, "time", "50"));
        filterOperator.addChildOPerator(new BasicOperator(SQLConstant.GREATERTHAN, "s1", "80"));

        ArrayList<String> paths = new ArrayList<>();
        paths.add("s1");
        paths.add("time");

        List<String> keys = new ArrayList<>();
        keys.add("");

        List<TSQueryPlan> queryPlans = new QueryProcessor().generatePlans(filterOperator, paths, keys, in, Long.valueOf("0"), Long.valueOf("749"));

        ArrayList<String> expectedPaths1 = new ArrayList<>();
        expectedPaths1.add("D:d2+C:c2+V:v1.s1");
        FilterOperator expectedTimeFilterOperator1 = new BasicOperator(SQLConstant.GREATERTHAN, "time", "50");
        FilterOperator expectedValueFilterOperator1 = new BasicOperator(SQLConstant.GREATERTHAN, "s1", "80");
        TSQueryPlan expectedQueryPlan1 = new TSQueryPlan(expectedPaths1, expectedTimeFilterOperator1, expectedValueFilterOperator1);

        ArrayList<String> expectedPaths2 = new ArrayList<>();
        expectedPaths2.add("D:d2+C:c2+V:v2.s1");
        FilterOperator expectedTimeFilterOperator2 = new BasicOperator(SQLConstant.GREATERTHAN, "time", "50");
        FilterOperator expectedValueFilterOperator2 = new BasicOperator(SQLConstant.GREATERTHAN, "s1", "80");
        TSQueryPlan expectedQueryPlan2 = new TSQueryPlan(expectedPaths2, expectedTimeFilterOperator2, expectedValueFilterOperator2);

        ArrayList<String> expectedPaths3 = new ArrayList<>();
        expectedPaths3.add("D:d1+C:c2+V:v1.s1");
        FilterOperator expectedTimeFilterOperator3 = new BasicOperator(SQLConstant.GREATERTHAN, "time", "50");
        FilterOperator expectedValueFilterOperator3 = new BasicOperator(SQLConstant.GREATERTHAN, "s1", "80");
        TSQueryPlan expectedQueryPlan3 = new TSQueryPlan(expectedPaths3, expectedTimeFilterOperator3, expectedValueFilterOperator3);

        Assert.assertEquals(3, queryPlans.size());
        Assert.assertEquals(expectedQueryPlan1.toString(), queryPlans.get(0).toString());
        Assert.assertEquals(expectedQueryPlan2.toString(), queryPlans.get(1).toString());
        Assert.assertEquals(expectedQueryPlan3.toString(), queryPlans.get(2).toString());

    }
}
