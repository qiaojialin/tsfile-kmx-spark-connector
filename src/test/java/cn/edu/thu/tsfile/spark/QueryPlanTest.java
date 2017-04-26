package cn.edu.thu.tsfile.spark;


import cn.edu.thu.tsfile.spark.common.BasicOperator;
import cn.edu.thu.tsfile.spark.common.FilterOperator;
import cn.edu.thu.tsfile.spark.common.TSQueryPlan;
import cn.edu.thu.tsfile.spark.exception.QueryProcessorException;
import cn.edu.thu.tsfile.timeseries.read.LocalFileInput;
import cn.edu.thu.tsfile.timeseries.read.qp.SQLConstant;
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
        new CreateTSFile().createTSFile1(tsfilePath);
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

        List<TSQueryPlan> queryPlans = new QueryProcessor().generatePlans(filterOperator, paths, in, Long.valueOf("0"), Long.valueOf("749"));

        ArrayList<String> expectedPaths1 = new ArrayList<>();
        expectedPaths1.add("root.car.d2.s1");
        FilterOperator expectedTimeFilterOperator1 = new BasicOperator(SQLConstant.GREATERTHAN, "time", "50");
        FilterOperator expectedValueFilterOperator1 = new BasicOperator(SQLConstant.GREATERTHAN, "s1", "80");
        TSQueryPlan expectedQueryPlan1 = new TSQueryPlan(expectedPaths1, expectedTimeFilterOperator1, expectedValueFilterOperator1);

        ArrayList<String> expectedPaths2 = new ArrayList<>();
        expectedPaths2.add("root.car.d1.s1");
        FilterOperator expectedTimeFilterOperator2 = new BasicOperator(SQLConstant.GREATERTHAN, "time", "50");
        FilterOperator expectedValueFilterOperator2 = new BasicOperator(SQLConstant.GREATERTHAN, "s1", "80");
        TSQueryPlan expectedQueryPlan2 = new TSQueryPlan(expectedPaths2, expectedTimeFilterOperator2, expectedValueFilterOperator2);

        Assert.assertEquals(2, queryPlans.size());
        Assert.assertEquals(expectedQueryPlan1.toString(), queryPlans.get(0).toString());
        Assert.assertEquals(expectedQueryPlan2.toString(), queryPlans.get(1).toString());
    }
}
