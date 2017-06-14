package cn.edu.thu.kvtsfile.qp.optimizer;

import cn.edu.thu.kvtsfile.qp.exception.DNFOptimizeException;
import cn.edu.thu.kvtsfile.qp.exception.MergeFilterException;
import cn.edu.thu.kvtsfile.qp.common.FilterOperator;
import cn.edu.thu.kvtsfile.qp.exception.RemoveNotException;

/**
 * provide a filter operator, optimize it.
 * 
 * @author kangrong
 *
 */
public interface IFilterOptimizer {
    FilterOperator optimize(FilterOperator filter) throws RemoveNotException, DNFOptimizeException, MergeFilterException;
}
