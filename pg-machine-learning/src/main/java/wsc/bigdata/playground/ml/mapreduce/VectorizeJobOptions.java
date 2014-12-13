package wsc.bigdata.playground.ml.mapreduce;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;

import wsc.bigdata.playground.common.CommandOptions;

/**
 * Define the options and arguments for VectorizeJob
 *
 */
public class VectorizeJobOptions extends MapReduceJobOptions {
  protected String columns;
  
  public VectorizeJobOptions() {
    super();
    createExtraOptions();
  }
  
  public void createExtraOptions() {
    Option columnsOption = CommandOptions.createOption("c", "columns", "Specify Columns to Parse", true, "columns", true, null);
    add(columnsOption);
  }

  public void populate(String[] args) throws ParseException {
    super.populate(args);
    CommandLine cmd = parse(args);
    columns = cmd.getOptionValue("c");
  }
  
  public String getColumns() {
    return columns;
  }
}
