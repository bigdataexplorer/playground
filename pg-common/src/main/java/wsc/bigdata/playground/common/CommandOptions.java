package wsc.bigdata.playground.common;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This helper class is built on top of commons cli to facilitate the creation of
 * different program argument options
 *
 */
public class CommandOptions {
  public static final Logger logger = LoggerFactory.getLogger(CommandOptions.class);
  private final Options options;

  public CommandOptions() {
    options = new Options();
  }

  /**
   * Return an Option 
   * @param name the option name
   * @param longName the long option name
   * @param description the description about the option
   * @param hasArg true if the option needs an additional argument
   * @param argName the argument name 
   * @param required
   * @param defaultName
   * @return
   */
  @SuppressWarnings("static-access")
  public static Option createOption(String name, String longName, String description,
      boolean hasArg, String argName, boolean required, String defaultName) {
    Option option = null;
    if (!hasArg) {
      option =
          OptionBuilder.isRequired(required).withDescription(description).withLongOpt(longName)
              .create(name);
    } else {
      option =
          OptionBuilder.withArgName(argName).isRequired(required).hasArg().withLongOpt(longName)
              .withDescription(description).create(name);
    }
    return option;
  }

  /**
   * Add an option to the option list
   * @param option
   */
  public void add(Option option) {
    options.addOption(option);
  }

  public Options getOptions() {
    return options;
  }

  public void usage(String headline) {
    HelpFormatter help = new HelpFormatter();
    logger.error("The arguments are wrong, please provide the required arguments as follows");
    help.printHelp(headline, options);
  }

  public void usageExit(String headline) {
    usage(headline);
    System.exit(1);
  }

  /**
   * Parse the program arguments
   * @param args the program args
   * @return CommandLine 
   * @throws ParseException
   */
  public CommandLine parse(String[] args) throws ParseException {
    CommandLineParser parser = new PosixParser();
    CommandLine commandLine = parser.parse(options, args);
    return commandLine;
  }
}
