/*
 * Gregor Münker
 */

 package org.eclipse.edc.makochain;

 import org.eclipse.edc.runtime.metamodel.annotation.Inject;
 import org.eclipse.edc.spi.system.ServiceExtension;
 import org.eclipse.edc.spi.system.ServiceExtensionContext;
 import org.eclipse.edc.web.spi.WebService;
 
 /**
  * Extension to initialize the policies.
  */
 public class RequestNewProviderExtension implements ServiceExtension {
 
     @Override
     public String name() {
         return "Request new Provider";
     }
     
     @Inject
     WebService webService;
 
     /**
      * Initializes the extension by binding the policies to the rule binding registry.
      *
      * @param context service extension context.
      */
     @Override
     public void initialize(ServiceExtensionContext context) {
         webService.registerResource(new RequestNewProvider(context.getMonitor()));
     }
 
 }
 