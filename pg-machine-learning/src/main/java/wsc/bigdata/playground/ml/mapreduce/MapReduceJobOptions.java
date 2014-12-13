package wsc.bigdata.playground.ml.mapreduce;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;

import wsc.bigdata.playground.common.CommandOptions;

public class MapReduceJobOptions extends CommandOptions {
  protected String inputPath;
  protected String outputPath;
  protected int numberTokens;
  protected String inputDelimiter = "\\|";
  protected String outputDelimiter = "|";
  
  public MapReduceJobOptions() {
    createOptions();
  }

  public void createOptions() {
    Option inputPathOption = CommandOptions.createOption("i", "input", "Specify Input Path", true, "inputPath", true, null);
    add(inputPathOption);
    Option outputPathOption = CommandOptions.createOption("o", "output", "Specify Output Path", true, "outputPath", true, null);
    add(outputPathOption);
    Option inputDelimiterOption = CommandOptions.createOption("d", "inputDelimiter", "Specify Input Delimiter", true, "inputDelimiter", true, null);
    add(inputDelimiterOption);
    Option outputDelimiterOption = CommandOptions.createOption("e", "outputDelimiter", "Specify Output Delimiter (optional)", true, "outputDelimiter", false, null);
    add(outputDelimiterOption);
    Option numberTokensOption = CommandOptions.createOption("n", "numberTokens", "Specify Number of Tokens", true, "numberTokens", true, null);
    add(numberTokensOption);
  }
  
  public void populate(String[] args) throws ParseException {
    CommandLine cmd = parse(args);
    inputPath = cmd.getOptionValue("i");
    outputPath = cmd.getOptionValue("o");
    numberTokens = Integer.parseInt(cmd.getOptionValue("n"));
    inputDelimiter = cmd.getOptionValue("d");
    if(cmd.hasOption("e")) {
      outputDelimiter = cmd.getOptionValue("e");
    }
  }

  public String getOutputPath() {
    return outputPath;
  }

  public String getInputPath() {
    return inputPath;
  }

  public int getNumberTokens() {
    return numberTokens;
  }

  public String getInputDelimiter() {
    return inputDelimiter;
  }

  public String getOutputDelimiter() {
    return outputDelimiter;
  }
}
