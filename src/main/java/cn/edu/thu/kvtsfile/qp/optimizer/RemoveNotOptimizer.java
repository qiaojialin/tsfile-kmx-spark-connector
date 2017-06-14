package cn.edu.thu.kvtsfile.qp.optimizer;


import cn.edu.thu.kvtsfile.qp.common.BasicOperator;
import cn.edu.thu.kvtsfile.qp.common.FilterOperator;
import cn.edu.thu.kvtsfile.qp.common.SQLConstant;
import cn.edu.thu.kvtsfile.qp.exception.BasicOperatorException;
import cn.edu.thu.kvtsfile.qp.exception.RemoveNotException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


public class RemoveNotOptimizer implements IFilterOptimizer {
    private static final Logger LOG = LoggerFactory.getLogger(RemoveNotOptimizer.class);

    /**
     * get DNF(disjunctive normal form) for this filter operator tree. Before getDNF, this op tree
     * must be binary, in another word, each non-leaf node has exactly two children.
     * 
     * @return
     * @throws RemoveNotException
     */
    @Override
    public FilterOperator optimize(FilterOperator filter) throws RemoveNotException {
        return removeNot(filter);
    }

    private FilterOperator removeNot(FilterOperator filter) throws RemoveNotException {
        if (filter.isLeaf())
            return filter;
        int tokenInt = filter.getTokenIntType();
        switch (tokenInt) {
            case SQLConstant.KW_AND:
            case SQLConstant.KW_OR:
                // replace children in-place for efficiency
                List<FilterOperator> children = filter.getChildren();
                children.set(0, removeNot(children.get(0)));
                children.set(1, removeNot(children.get(1)));
                return filter;
            case SQLConstant.KW_NOT:
                try {
                    return reverseFilter(filter.getChildren().get(0));
                } catch (BasicOperatorException e) {
                    LOG.error("reverse Filter failed.");
                }
            default:
                throw new RemoveNotException("Unknown token in removeNot: " + tokenInt + ","
                        + SQLConstant.tokenNames.get(tokenInt));
        }
    }


    private FilterOperator reverseFilter(FilterOperator filter) throws RemoveNotException, BasicOperatorException {
        int tokenInt = filter.getTokenIntType();
        if (filter.isLeaf()) {
            try {
                ((BasicOperator) filter).setReversedTokenIntType();
            } catch (BasicOperatorException e) {
                throw new RemoveNotException(
                        "convert BasicFuntion to reserved meet failed: previous token:" + tokenInt
                                + "tokenSymbol:" + SQLConstant.tokenNames.get(tokenInt));
            }
            return filter;
        }
        switch (tokenInt) {
            case SQLConstant.KW_AND:
            case SQLConstant.KW_OR:
                List<FilterOperator> children = filter.getChildren();
                children.set(0, reverseFilter(children.get(0)));
                children.set(1, reverseFilter(children.get(1)));
                filter.setTokenIntType(SQLConstant.reverseWords.get(tokenInt));
                return filter;
            case SQLConstant.KW_NOT:
                return removeNot(filter.getChildren().get(0));
            default:
                throw new RemoveNotException("Unknown token in reverseFilter: " + tokenInt + ","
                        + SQLConstant.tokenNames.get(tokenInt));
        }
    }

}
