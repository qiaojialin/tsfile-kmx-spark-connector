package cn.edu.thu.kvtsfile.qp;

import cn.edu.thu.tsfile.common.utils.TSRandomAccessFileReader;
import cn.edu.thu.tsfile.timeseries.read.query.QueryConfig;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfile.timeseries.read.query.QueryEngine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * This class used to execute Querys on TSFile
 */
public class Executor {
    public static List<QueryDataSet> query(TSRandomAccessFileReader in, List<QueryConfig> queryConfigs, HashMap<String, Long> parameters) {
        QueryEngine queryEngine;
        List<QueryDataSet> dataSets = new ArrayList<>();
        try {
            queryEngine = new QueryEngine(in);
            for(QueryConfig queryConfig: queryConfigs) {
                QueryDataSet queryDataSet = queryEngine.query(queryConfig, parameters);
                dataSets.add(queryDataSet);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return dataSets;
    }
}
