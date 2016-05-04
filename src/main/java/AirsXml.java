package src.main.java;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.Characters;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;

import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.DeleteResult;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.SaveResult;
import com.sforce.soap.partner.Error;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

import org.apache.log4j.Logger;

public class AirsXml {
    private static final Logger log = Logger.getLogger(AirsXml.class.getName());
    static final String USERNAME = "mtran@211sandiego.org.dev";
    static final String PASSWORD = "m1nh@211KsmlvVA4mvtI6YwzKZOLjbKF9";
    static PartnerConnection connection;

    static void usage() {
        System.err.println("");
        System.err.println("usage: java -jar AirsXml.jar <airs.xml>");
        System.err.println("");
        System.exit(-1);
    }

    public static void main(String[] args) {
        if (args.length == 0 || args.length < 1) {
            usage();
        }

        // Establish DB connection in order to look up taxonomy name.
        Connection sqlConn = DbUtils.getDBConnection();
        if (sqlConn == null) {
            System.exit(-1);
        }

    	ConnectorConfig config = new ConnectorConfig();
    	config.setUsername(USERNAME);
    	config.setPassword(PASSWORD);
    	//config.setTraceMessage(true);

        try {
            // Establish Salesforce web service connection.
    		connection = Connector.newConnection(config);

    		// @debug.
    		log.info("Auth EndPoint: " + config.getAuthEndpoint());
    		log.info("Service EndPoint: " + config.getServiceEndpoint());
    		log.info("Username: " + config.getUsername());
    		log.info("SessionId: " + config.getSessionId());

            // Parse AIRS XML file.
            String fileName = args[0];
            log.info("Parsing XML document (" + fileName + ")...\n");
            parseXmlFile(sqlConn, fileName);
        }
    	catch (ConnectionException e) {
            log.error(e.getMessage());
            e.printStackTrace();
    	}
        catch (Exception e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
        finally {
            DbUtils.closeConnection(sqlConn);
        }
    }

    private static void parseXmlFile(Connection sqlConn, String inputFile) {
        try {
        	// First create a new XMLInputFactory.
        	XMLInputFactory inputFactory = XMLInputFactory.newInstance();

            // Set this property to handle special HTML characters like & etc.
            inputFactory.setProperty(XMLInputFactory.IS_COALESCING, true);

        	// Setup a new eventReader.
        	InputStream in = new FileInputStream(inputFile);
        	XMLEventReader eventReader = inputFactory.createXMLEventReader(in);

            boolean agencyInfo = false;
            boolean agencyName = false;
            boolean agencyAddr = false;
            boolean agencyCity = false;
            boolean agencyState = false;
            boolean agencyZip = false;

            boolean siteInfo = false;
            boolean sitePhoneInfo = false;
            boolean resourceInfo = false;
            boolean serviceName = false;
            boolean serviceDescription = false;
            boolean code = false;
            boolean contactName = false;
            boolean contactPhone = false;
            boolean contactEmail = false;
            boolean requirements = false;

            // Query referral agency record type id.
		    String referralRecTypeId = queryRecordType("Referral Agency");
            String agencyId = null;
            AgencyInfo agency = null;
            ServiceInfo service = null;

        	// Read the XML document.
        	while (eventReader.hasNext()) {
        		XMLEvent event = eventReader.nextEvent();
                String qName = "";
                switch (event.getEventType()) {
                    case XMLStreamConstants.START_ELEMENT:
                        StartElement startEle = event.asStartElement();
                        qName = startEle.getName().getLocalPart();
                        switch (qName) {
                            case "AgencyLocation":
                                log.info("Start element: AgencyLocation");
                                agencyInfo = true;
                                agency = new AgencyInfo();
                                break;
                            case "Line1":
                                if (agencyInfo) {
                                    agencyAddr = true;
                                }
                                break;
                            case "City":
                                if (agencyInfo) {
                                    agencyCity = true;
                                }
                                break;
                            case "State":
                                if (agencyInfo) {
                                    agencyState = true;
                                }
                                break;
                            case "ZipCode":
                                if (agencyInfo) {
                                    agencyZip = true;
                                }
                                break;
                            case "SiteService":
                                log.info("Start element: SiteService");
                                siteInfo = true;
                                service = new ServiceInfo();
                                break;
                            case "ResourceInfo":
                                resourceInfo = true;
                                siteInfo = false;
                                break;
                            case "Phone":
                                if (siteInfo) {
                                    sitePhoneInfo = true;
                                    siteInfo = false;
                                }
                                break;
                            case "Description":
                                if (siteInfo) {
                                    serviceDescription = true;
                                }
                                break;
                            case "Code":
                                code = true;
                                break;
                            case "Name":
                                if (agencyInfo) {
                                    agencyName = true;
                                }
                                else if (siteInfo) {
                                    serviceName = true;
                                }
                                else if (resourceInfo) {
                                    contactName = true;
                                }
                                break;
                            case "PhoneNumber":
                                if (sitePhoneInfo) {
                                    contactPhone = true;
                                }
                                break;
                            case "Address": // Email address.
                                if (resourceInfo) {
                                    contactEmail = true;
                                }
                                break;
                            case "OtherRequirements":
                                requirements = true;
                                break;
                            default:
                                break;
                        }
                        break;
                    case XMLStreamConstants.CHARACTERS:
                        Characters chars = event.asCharacters();
                        if (agencyName) {
                            if (agency != null) {
                                agency.agencyName = chars.getData();
                                log.info("Agency name: " + agency.agencyName);
                            }
                            agencyName = false;
                        }
                        else if (agencyAddr) {
                            if (agency != null) {
                                agency.agencyAddr = chars.getData();
                                log.info("Agency address: " + agency.agencyAddr);
                            }
                            agencyAddr = false;
                        }
                        else if (agencyCity) {
                            if (agency != null) {
                                agency.agencyCity = chars.getData();
                                log.info("Agency city: " + agency.agencyCity);
                            }
                            agencyCity = false;
                        }
                        else if (agencyState) {
                            if (agency != null) {
                                agency.agencyState = chars.getData();
                                log.info("Agency state: " + agency.agencyState);
                            }
                            agencyState = false;
                        }
                        else if (agencyZip) {
                            if (agency != null) {
                                agency.agencyZip = chars.getData();
                                log.info("Agency zip code: " + agency.agencyZip);
                            }
                            agencyZip = false;
                        }
                        else if (serviceName) {
                            if (service != null) {
                                service.serviceName = chars.getData();
                                log.info("Service name: " + service.serviceName);
                            }
                            serviceName = false;
                        }
                        else if (serviceDescription) {
                            if (service != null) {
                                String desc = chars.getData();
                                service.serviceDescription = desc.replaceAll("\\r|\\n", "");
                                log.info("Service description: " + service.serviceDescription);
                            }
                            serviceDescription = false;
                        }
                        else if (code) {
                            if (service != null) {
                                String s = chars.getData();
                                service.taxonomyCodes.add(s.trim());
                                log.info("Taxonomy Code: " + s);
                            }
                            code = false;
                        }
                        else if (contactName) {
                            if (agency != null) {
                                agency.contactName = chars.getData();
                                log.info("Contact name: " + agency.contactName);
                            }
                            contactName = false;
                        }
                        else if (contactPhone) {
                            if (agency != null) {
                                agency.contactPhone = chars.getData();
                                log.info("Contact phone: " + agency.contactPhone);
                            }
                            contactPhone = false;
                        }
                        else if (contactEmail) {
                            if (agency != null) {
                                agency.contactEmail = chars.getData();
                                log.info("Contact email: " + agency.contactEmail);
                            }
                            contactEmail = false;
                        }
                        else if (requirements) {
                            if (service != null) {
                                service.requirements = chars.getData();
                                log.info(service.requirements);
                            }
                            requirements = false;
                        }
                        break;
                    case XMLStreamConstants.END_ELEMENT:
                        EndElement endEle = event.asEndElement();
                        qName = endEle.getName().getLocalPart();
                        switch (qName) {
                            //case "AgencyLocation":
                            case "PhysicalAddress":
                                if (agencyInfo) {
                                    log.info("End element: AgencyLocation\n");
                                    agencyInfo = false;

                                    // Insert/update agency.
                                    agencyId = upsertAgency(referralRecTypeId, agency);
                                }
                                break;
                            case "SiteService":
                                log.info("End element: SiteService\n");
                                siteInfo = false;

                                // Has service been created?
                                String serviceId = queryService(agencyId, service.serviceName);
                                if (serviceId == null) {
                                    // Create new service.
                                    serviceId = createService(agencyId, service);
                                }

                                // Check to see if taxonomy codes have been created for this service..
                                List<String> newCodes = new ArrayList<String>();
                                for (int k = 0; k < service.taxonomyCodes.size(); k++) {
                                    String taxCode = service.taxonomyCodes.get(k);
                                    String needId = queryNeed(serviceId, taxCode);
                                    if (needId == null) {
                                        newCodes.add(taxCode);
                                    }
                                }

                                // Create new taxonomy codes (needs).
                                createNeeds(sqlConn, serviceId, newCodes);
                                service = null;
                                break;
                            case "ResourceInfo":
                                resourceInfo = false;
                                break;
                            case "Phone":
                                sitePhoneInfo = false;
                                break;
                            default:
                                break;
                        }
                        break;
                    default:
                        break;
                }
        	}
        }
        catch (FileNotFoundException e) {
            log.error("XML file document not found!\n");
        }
        catch (XMLStreamException e) {
            log.error("Error parsing XML document:\n");
        	e.printStackTrace();
        }
    }

    private static String queryRecordType(String name) {
    	log.info("Querying for Referral Agency record type...");
        String recordTypeId = null;
    	try {
            // Query for record type name.
    		String sql = "SELECT Id, Name FROM RecordType " +
                         "WHERE Name = '" + name + "' ";
    		QueryResult queryResults = connection.query(sql);
    		if (queryResults.getSize() > 0) {
    			for (SObject s: queryResults.getRecords()) {
                    recordTypeId = s.getId();
    			}
    		}
    	}
    	catch (Exception e) {
    		e.printStackTrace();
    	}

        // @debug.
        if (recordTypeId != null) {
			log.info("Record Type Id: " + recordTypeId);
        }
        else {
            log.info("Referral agency record type not found!");
        }

        return recordTypeId;
    }

    private static String queryService(String agencyId, String name) {
    	log.info("Querying for service name: " + name + "...");
        String serviceId = null;
    	try {
    		String sql = "SELECT Id, Name, Service_Name__c " +
    					 "FROM Service__c " +
    					 "WHERE Agency__r.Id = '" + agencyId + "' " +
    					 "  AND Service_Name__c = '" + name + "'";
    		QueryResult queryResults = connection.query(sql);
    		if (queryResults.getSize() > 0) {
    			for (SObject s: queryResults.getRecords()) {
    				serviceId = s.getId();
    			}
    		}
    	}
    	catch (Exception e) {
    		e.printStackTrace();
    	}

        // @debug.
        if (serviceId != null) {
			log.info("Service Id: " + serviceId);;
        }
        else {
            log.info("Service name not found!");
        }

        return serviceId;
    }

    private static String queryNeed(String serviceId, String name) {
    	log.info("Querying for need name: " + name + "...");
        String needId = null;
    	try {
    		String sql = "SELECT Id, Name, Taxonomy_Code__c " +
    					 "FROM Need__c " +
    					 "WHERE Service__r.Id = '" + serviceId + "' " +
    					 "  AND Taxonomy_Code__c = '" + name + "'";
    		QueryResult queryResults = connection.query(sql);
    		if (queryResults.getSize() > 0) {
    			for (SObject s: queryResults.getRecords()) {
    				needId = s.getId();
    			}
    		}
    	}
    	catch (Exception e) {
    		e.printStackTrace();
    	}

        // @debug.
        if (needId != null) {
			log.info("Need Id: " + needId);;
        }
        else {
            log.info("Need name not found!");
        }

        return needId;
    }

    private static String upsertAgency(String recordTypeId, AgencyInfo ai) {
        String agencyId = null;
    	try {
            SObject[] records = new SObject[1];
            SaveResult[] saveResults = null;

            // Has agency been created before?
    		String sql = "SELECT Id, Name FROM Account " +
                         "WHERE Name = '" + ai.agencyName + "' ";
    		QueryResult queryResults = connection.query(sql);
    		if (queryResults.getSize() > 0) {
				SObject so = (SObject)queryResults.getRecords()[0];
                log.info("Updating agency id: " + so.getId() + " - name: "+ so.getField("Name"));

				SObject soUpdate = new SObject();
				soUpdate.setType("Account");
		        so.setField("RecordTypeId", recordTypeId);
				soUpdate.setId(so.getId());
				soUpdate.setField("Name", so.getField("Name"));
                soUpdate.setField("BillingStreet", ai.agencyAddr);
                soUpdate.setField("BillingCity", ai.agencyCity);
                soUpdate.setField("BillingState", ai.agencyState);
                soUpdate.setField("BillingPostalCode", ai.agencyZip);
                soUpdate.setField("Phone", ai.contactPhone);
                records[0] = soUpdate;

                // Update agency.
                saveResults = connection.update(records);
            }
            else {
                log.info("Creating new agency name: " + ai.agencyName);
			    SObject so = new SObject();
				so.setType("Account");
		        so.setField("RecordTypeId", recordTypeId);
				so.setField("Name", ai.agencyName);
                so.setField("BillingStreet", ai.agencyAddr);
                so.setField("BillingCity", ai.agencyCity);
                so.setField("BillingState", ai.agencyState);
                so.setField("BillingPostalCode", ai.agencyZip);
                so.setField("Phone", ai.contactPhone);
                records[0] = so;

                // Create new agency.
                saveResults = connection.create(records);
            }

    		// Check the returned results for any errors.
    		for (int i = 0; i < saveResults.length; i++) {
    			if (saveResults[i].isSuccess()) {
    				agencyId = saveResults[i].getId();
    				log.info(i + ". Successfully created/updated record - Id: " + agencyId);
    			}
    			else {
    				Error[] errors = saveResults[i].getErrors();
    				for (int j=0; j< errors.length; j++) {
    					log.info("ERROR inserting/updating record: " + errors[j].getMessage());
    				}
    			}
    		}
    	}
    	catch (Exception e) {
    		e.printStackTrace();
    	}

        return agencyId;
    }

    private static String createService(String agencyId, ServiceInfo si) {
        String serviceId = null;
    	try {
    		SObject so = new SObject();
    		so.setType("Service__c");
    		so.setField("Agency__c", agencyId);
    		so.setField("Service_Name__c", si.serviceName);
    		so.setField("Service_Description__c", si.serviceDescription);

            SObject[] records = new SObject[1];
    		records[0] = so;

    		// Create service record in Salesforce.com.
    		SaveResult[] saveResults = connection.create(records);

    		// Check the returned results for any errors.
    		for (int i = 0; i < saveResults.length; i++) {
    			if (saveResults[i].isSuccess()) {
    				serviceId = saveResults[i].getId();
    				System.out.println(i+". Successfully created record - Id: " + serviceId);
    			}
    			else {
    				Error[] errors = saveResults[i].getErrors();
    				for (int j = 0; j < errors.length; j++) {
    					System.out.println("Error creating record: " + errors[j].getMessage());
    				}
    			}
    		}
    	}
    	catch (Exception e) {
    		e.printStackTrace();
    	}

        return serviceId;
    }

    private static void createNeeds(Connection sqlConn, String serviceId, List<String> newCodes) {
    	try {
            int n = newCodes.size();
            if (n <= 0) {
                log.info("No taxonomy code found!");
                return;
            }

            SObject[] records = new SObject[n];
            for (int i = 0; i < n; i++) {
                String code = newCodes.get(i);

                // Look up taxonomy code name.
                DbTaxonomy db = DbTaxonomy.findByCode(sqlConn, code);

    		    SObject so = new SObject();
        		so.setType("Need__c");
        		so.setField("Service__c", serviceId);
        		so.setField("Taxonomy_Code__c", code);
                if (db != null) {
        		    so.setField("Taxonomy_Name__c", db.name);
                }
                records[i] = so;
            }

    		// Create need records in Salesforce.com.
    		SaveResult[] saveResults = connection.create(records);

    		// Check the returned results for any errors.
    		for (int i = 0; i < saveResults.length; i++) {
    			if (saveResults[i].isSuccess()) {
    				System.out.println(i+". Successfully created record - Id: " + saveResults[i].getId());
    			}
    			else {
    				Error[] errors = saveResults[i].getErrors();
    				for (int j = 0; j < errors.length; j++) {
    					System.out.println("Error creating record: " + errors[j].getMessage());
    				}
    			}
    		}
    	}
    	catch (Exception e) {
    		e.printStackTrace();
    	}
    }
}
