package cn.edu.thu.kvtsfile.spark.optimizer;

import cn.edu.thu.kvtsfile.spark.common.*;
import cn.edu.thu.tsfile.common.constant.SystemConstant;
import cn.edu.thu.tsfile.common.utils.TSRandomAccessFileReader;
import cn.edu.thu.tsfile.timeseries.read.metadata.SeriesSchema;
import cn.edu.thu.tsfile.timeseries.read.query.QueryEngine;

import java.io.IOException;
import java.util.*;

/**
 * Created by qiaojialin on 2017/4/26.
 */
public class PhysicalOptimizer {

    //determine whether to query all delta_objects from TSFile. true means do query.
    private boolean flag;
    private List<String> validDeltaObjects = new ArrayList<>();

    public List<TSQueryPlan> optimize(SingleQuery singleQuery, List<String> paths, List<String> keys, TSRandomAccessFileReader in, Long start, Long end) throws IOException {

        List<String> validPaths = new ArrayList<>();
        for(String path: paths) {
            if(!keys.contains(path) && !path.equals(SQLConstant.RESERVED_TIME)) {
                validPaths.add(path);
            }
        }
        FilterOperator timeFilter = null;
        FilterOperator valueFilter = null;

        QueryEngine queryEngine = new QueryEngine(in);

        //get all series in current tsfile
        List<SeriesSchema> actualSeriesList = queryEngine.getAllSeriesSchema();
        List<String> actualDeltaObjectList = queryEngine.getAllDeltaObjectUIDByPartition(start, end);

        if(singleQuery != null) {
            timeFilter = singleQuery.getTimeFilterOperator();
            valueFilter = singleQuery.getValueFilterOperator();
            if (valueFilter != null) {
                List<String> filterPaths = valueFilter.getAllPaths();
                List<String> actualPaths = new ArrayList<>();
                for (SeriesSchema series : actualSeriesList) {
                    actualPaths.add(series.name);
                }

                //if filter paths doesn't in tsfile, don't query
                if (!actualPaths.containsAll(filterPaths))
                    return new ArrayList<>();
            }

            flag = true;
            List<Set<String>> selectkeys = mergeKeys(singleQuery.getKeyFilterOperators());
            if(!flag) {
                return new ArrayList<>();
            }

            //if select keys, then match with measurement
            if(!selectkeys.isEmpty()) {
                combination(actualDeltaObjectList, selectkeys, 0, new String[selectkeys.size()]);
            } else {
                validDeltaObjects.addAll(actualDeltaObjectList);
            }
        } else {
            validDeltaObjects.addAll(actualDeltaObjectList);
        }

        Set<String> seriesSet = new HashSet<>();
        for(SeriesSchema series: actualSeriesList) {
            seriesSet.add(series.name);
        }

        //query all measurements from TSFile
        if(validPaths.size() == 0) {
            for(SeriesSchema series: actualSeriesList) {
                validPaths.add(series.name);
            }
        } else {
            //remove paths that doesn't exist in file
            validPaths.removeIf(path -> !seriesSet.contains(path));
        }


        List<TSQueryPlan> tsFileQueries = new ArrayList<>();
        for(String deltaObject: validDeltaObjects) {
            List<String> newPaths = new ArrayList<>();
            for(String path: validPaths) {
                String newPath = deltaObject + SystemConstant.PATH_SEPARATOR + path;
                newPaths.add(newPath);
            }
            if(valueFilter == null) {
                tsFileQueries.add(new TSQueryPlan(newPaths, timeFilter, null));
            } else {
                FilterOperator newValueFilter = valueFilter.clone();
                newValueFilter.addHeadDeltaObjectPath(deltaObject);
                tsFileQueries.add(new TSQueryPlan(newPaths, timeFilter, newValueFilter));
            }
        }
        return tsFileQueries;
    }

    /**
     * calculate combinations of selected keys and add valid deltaObjects to validDeltaObjects
     *
     * @param actualDeltaObjects deltaObjects from file
     * @param inputList (D:d1, D:d2) (C:c1, C:c2)
     * @param beginIndex current recursion list index
     * @param arr combination of inputList
     */
    private void combination(List<String> actualDeltaObjects, List<Set<String>> inputList, int beginIndex, String[] arr) {
        if(beginIndex == inputList.size()){
            for(String deltaObject: actualDeltaObjects) {
                boolean valid = true;
                for (String kv : arr) {
                    if(!deltaObject.contains(kv))
                        valid = false;
                }
                if(valid)
                    validDeltaObjects.add(deltaObject);
            }
            return;
        }

        for(String c: inputList.get(beginIndex)){
            arr[beginIndex] = c;
            combination(actualDeltaObjects, inputList, beginIndex + 1, arr);
        }
    }

    private List<Set<String>> mergeKeys(List<FilterOperator> keyFilterOperators) {
        List<Set<String>> keys = new ArrayList<>();
        for(FilterOperator filterOperator: keyFilterOperators) {
            Set<String> values = mergeKey(filterOperator);
            if (values!= null && !values.isEmpty())
                keys.add(values);
        }
        return keys;
    }

    /**
     * merge one key filterOperator
     * @param keyFilterOperator key filter
     * @return selected values of the key filter
     */
    private Set<String> mergeKey(FilterOperator keyFilterOperator) {
        if (keyFilterOperator == null) {
            return null;
        }
        if (keyFilterOperator.isLeaf()) {
            Set<String> ret = new HashSet<>();
            ret.add(keyFilterOperator.getSinglePath() +
                    SparkConstant.DELTA_OBJECT_VALUE_SEPARATOR +
                    ((BasicOperator)keyFilterOperator).getSeriesValue());
            return ret;
        }
        List<FilterOperator> children = keyFilterOperator.getChildren();
        if (children == null || children.isEmpty()) {
            return new HashSet<>();
        }

        Set<String> ret = mergeKey(children.get(0));
        if(ret == null){
            return null;
        }
        for (int i = 1; i < children.size(); i++) {
            Set<String> temp = mergeKey(children.get(i));
            if(temp == null) {
                return null;
            }
            switch (keyFilterOperator.getTokenIntType()) {
                case SQLConstant.KW_AND:
                    ret.retainAll(temp);
                    //example: "where key1 = d1 and key1 = d2" should not query data
                    if(ret.isEmpty()) {
                        flag = false;
                    }
                    break;
                case SQLConstant.KW_OR:
                    ret.addAll(temp);
                    break;
                default:
                    throw new UnsupportedOperationException("given error token type:" + keyFilterOperator.getTokenIntType());
            }
        }
        return ret;
    }

}
