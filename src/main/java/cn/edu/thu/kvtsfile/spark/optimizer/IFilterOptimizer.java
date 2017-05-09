package cn.edu.thu.kvtsfile.spark.optimizer;

import cn.edu.thu.kvtsfile.spark.common.FilterOperator;
import cn.edu.thu.kvtsfile.spark.exception.DNFOptimizeException;
import cn.edu.thu.kvtsfile.spark.exception.MergeFilterException;
import cn.edu.thu.kvtsfile.spark.exception.RemoveNotException;
import cn.edu.thu.kvtsfile.spark.common.FilterOperator;
import cn.edu.thu.kvtsfile.spark.exception.DNFOptimizeException;
import cn.edu.thu.kvtsfile.spark.exception.MergeFilterException;
import cn.edu.thu.kvtsfile.spark.exception.RemoveNotException;

/**
 * provide a filter operator, optimize it.
 * 
 * @author kangrong
 *
 */
public interface IFilterOptimizer {
    FilterOperator optimize(FilterOperator filter) throws RemoveNotException, DNFOptimizeException, MergeFilterException;
}
