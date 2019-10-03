package org.apache.hadoop.ozone.om;

import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.ozone.om.protocol.OzoneManagerProtocol;
import org.apache.hadoop.ozone.om.protocolPB.OzoneManagerProtocolClientSideTranslatorPB;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.ratis.protocol.ClientId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.io.IOException;
import java.util.Map;


/**
 * A command line tool for making calls in OM HA protocols.
 */
@Command(name = "ozone omha",
    hidden = true, description = "Command line tool for OM HA.",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true)
public class OzoneManagerHA extends GenericCli {
  private OzoneConfiguration conf;
  private static final Logger LOG =
      LoggerFactory.getLogger(OzoneManagerHA.class);

  public static void main(String[] args) throws Exception {
    TracingUtil.initTracing("OzoneManager");
    new OzoneManagerHA().run(args);
  }

  private OzoneManagerHA() {
    super();
  }

  /**
   * This function implements a sub-command to allow the OM to be
   * initialized from the command line.
   */
  @CommandLine.Command(name = "--getservicestate",
      customSynopsis = "ozone om [global options] --getservicestate " +
          "--serviceId=<OMServiceID>",
      hidden = false,
      description = "Get the Ratis server state of all OMs belonging to given" +
          " OM Service ID",
      mixinStandardHelpOptions = true,
      versionProvider = HddsVersionProvider.class)
  public void getRoleInfoOm(@CommandLine.Option(names = { "--serviceId" },
      description = "The OM Service ID of the OMs to get the server states for",
      paramLabel = "id") String serviceId)
      throws Exception {
    conf = createOzoneConfiguration();
    Map<String, String> serviceStates = getServiceStates(conf, serviceId);
    for (String nodeId : serviceStates.keySet()) {
      System.out.println(nodeId + " : " + serviceStates.get(nodeId));
    }
  }

  private Map<String, String> getServiceStates(OzoneConfiguration conf,
      String serviceId) throws IOException, AuthenticationException {

    OzoneManagerProtocol omProxy = TracingUtil.createProxy(
        new OzoneManagerProtocolClientSideTranslatorPB(conf,
            ClientId.randomId().toString(), serviceId,
            UserGroupInformation.getCurrentUser()),
        OzoneManagerProtocol.class, conf);

    return omProxy.getServiceStates(serviceId);
  }
}
