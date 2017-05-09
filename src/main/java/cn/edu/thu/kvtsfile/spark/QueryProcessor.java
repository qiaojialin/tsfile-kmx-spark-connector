package cn.edu.thu.kvtsfile.spark;

import cn.edu.thu.kvtsfile.spark.common.FilterOperator;
import cn.edu.thu.kvtsfile.spark.common.SingleQuery;
import cn.edu.thu.kvtsfile.spark.optimizer.RemoveNotOptimizer;
import cn.edu.thu.tsfile.common.utils.TSRandomAccessFileReader;
import cn.edu.thu.kvtsfile.spark.common.BasicOperator;
import cn.edu.thu.kvtsfile.spark.common.FilterOperator;
import cn.edu.thu.kvtsfile.spark.common.SingleQuery;
import cn.edu.thu.kvtsfile.spark.common.TSQueryPlan;
import cn.edu.thu.kvtsfile.spark.exception.QueryOperatorException;
import cn.edu.thu.kvtsfile.spark.exception.QueryProcessorException;
import cn.edu.thu.kvtsfile.spark.optimizer.DNFFilterOptimizer;
import cn.edu.thu.kvtsfile.spark.optimizer.MergeSingleFilterOptimizer;
import cn.edu.thu.kvtsfile.spark.optimizer.PhysicalOptimizer;
import cn.edu.thu.kvtsfile.spark.optimizer.RemoveNotOptimizer;
import cn.edu.thu.tsfile.timeseries.read.metadata.SeriesSchema;
import cn.edu.thu.tsfile.timeseries.read.qp.SQLConstant;
import cn.edu.thu.tsfile.timeseries.read.query.QueryEngine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


/**
 * This class is used to convert information given by sparkSQL to construct TSFile's query plans.
 * For TSFile's schema differ from SparkSQL's table schema
 * e.g.
 * TSFile's SQL: select s1,s2 from root.car.d1 where s1 = 10
 * SparkSQL's SQL: select s1,s2 from XXX where delta_object = d1
 *
 * @author QJL、KR
 */
public class QueryProcessor {

    /**
     * construct logical query plans first, then convert them to physical ones
     *
     * @param filter all filters
     * @param paths selected paths
     * @param keys device ids
     * @param in file input stream
     * @param start partition start offset
     * @param end partition end offset
     * @return a list of physical query plans
     * @throws QueryProcessorException
     * @throws IOException
     */
    public List<TSQueryPlan> generatePlans(FilterOperator filter, List<String> paths, List<String> keys, TSRandomAccessFileReader in, Long start, Long end) throws QueryProcessorException, IOException {

        List<TSQueryPlan> queryPlans = new ArrayList<>();

        if(filter != null) {
            RemoveNotOptimizer removeNot = new RemoveNotOptimizer();
            filter = removeNot.optimize(filter);

            DNFFilterOptimizer dnf = new DNFFilterOptimizer();
            filter = dnf.optimize(filter);
            MergeSingleFilterOptimizer merge = new MergeSingleFilterOptimizer();
            filter = merge.optimize(filter);

            List<FilterOperator> filterOperators = splitFilter(filter);

            for (FilterOperator filterOperator : filterOperators) {
                SingleQuery singleQuery = constructSelectPlan(filterOperator, keys);
                if (singleQuery != null) {
                    queryPlans.addAll(new PhysicalOptimizer().optimize(singleQuery, paths, keys, in, start, end));
                }
            }
        } else {
            queryPlans.addAll(new PhysicalOptimizer().optimize(null, paths, keys, in, start, end));
        }

        return queryPlans;
    }

    /**
     * split DNF into many CNF
     * @param filterOperator DNF
     * @return a list of CNFs
     */
    private List<FilterOperator> splitFilter(FilterOperator filterOperator) {
        if (filterOperator.isSingle() || filterOperator.getTokenIntType() != SQLConstant.KW_OR) {
            List<FilterOperator> ret = new ArrayList<>();
            ret.add(filterOperator);
            return ret;
        }
        // a list of conjunctions linked by or
        return filterOperator.childOperators;
    }

    /**
     * construct logical plans from CNF
     * @param filterOperator CNF
     * @param keys device ids
     * @return logical plans
     * @throws QueryOperatorException exception in filterOperator resolving
     */
    private SingleQuery constructSelectPlan(FilterOperator filterOperator, List<String> keys) throws QueryOperatorException {

        FilterOperator timeFilter = null;
        FilterOperator valueFilter = null;
        List<FilterOperator> keyFilterOperators = new ArrayList<>();

        List<FilterOperator> singleFilterList = null;

        if (filterOperator.isSingle()) {
            singleFilterList = new ArrayList<>();
            singleFilterList.add(filterOperator);

        } else if (filterOperator.getTokenIntType() == SQLConstant.KW_AND) {
            // original query plan has been dealt with merge optimizer, thus all nodes with same path have been
            // merged to one node
            singleFilterList = filterOperator.getChildren();
        }

        if(singleFilterList == null) {
            return null;
        }

        List<FilterOperator> valueList = new ArrayList<>();
        for (FilterOperator child : singleFilterList) {
            if (!child.isSingle()) {
                valueList.add(child);
            } else {
                String singlePath = child.getSinglePath();
                if(keys.contains(singlePath)) {
                    if (!keyFilterOperators.contains(child))
                        keyFilterOperators.add(child);
                    else
                        throw new QueryOperatorException(
                                "The same key filter has been specified more than once: " + singlePath);
                }else {
                    switch (child.getSinglePath()) {
                        case SQLConstant.RESERVED_TIME:
                            if (timeFilter != null) {
                                throw new QueryOperatorException(
                                        "time filter has been specified more than once");
                            }
                            timeFilter = child;
                            break;
                        default:
                            valueList.add(child);
                            break;
                    }
                }
            }
        }

        if (valueList.size() == 1) {
            valueFilter = valueList.get(0);

        } else if (valueList.size() > 1) {
            valueFilter = new FilterOperator(SQLConstant.KW_AND, false);
            valueFilter.childOperators = valueList;
        }

        return new SingleQuery(keyFilterOperators, timeFilter, valueFilter);
    }
}
