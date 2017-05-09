package cn.edu.thu.kvtsfile.spark.common;


import java.util.List;

/**
 * This class is constructed with a single getIndex plan. Single getIndex means it could be processed by
 * reading API by one pass directly.<br>
 *
 */
public class SingleQuery {

    private List<FilterOperator> keyFilterOperators;
    private FilterOperator timeFilterOperator;
    private FilterOperator valueFilterOperator;

    public SingleQuery(List<FilterOperator> keyFilterOperators,
                       FilterOperator timeFilter, FilterOperator valueFilter) {
        super();
        this.keyFilterOperators = keyFilterOperators;
        this.timeFilterOperator = timeFilter;
        this.valueFilterOperator = valueFilter;
    }

    public List<FilterOperator> getKeyFilterOperators() {

        return keyFilterOperators;
    }

    public FilterOperator getTimeFilterOperator() {
        return timeFilterOperator;
    }

    public FilterOperator getValueFilterOperator() {
        return valueFilterOperator;
    }

    @Override
    public String toString() {
        return "SingleQuery: \n" + keyFilterOperators + "\n" + timeFilterOperator + "\n" + valueFilterOperator;
    }


}
