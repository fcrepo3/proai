package proai.service;

import org.apache.log4j.Logger;
import proai.error.BadArgumentException;
import proai.error.BadVerbException;
import proai.error.ProtocolException;
import proai.error.ServerException;
import proai.util.StreamUtil;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class ProviderServlet extends HttpServlet {
    static final long serialVersionUID = 1;

    private static final Logger logger =
            Logger.getLogger(ProviderServlet.class.getName());

    /**
     * Every response starts with this string.
     */
    private static final String _PROC_INST = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n";
    private static final String _XMLSTART = "<OAI-PMH xmlns=\"http://www.openarchives.org/OAI/2.0/\"\n"
            + "         xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
            + "         xsi:schemaLocation=\"http://www.openarchives.org/OAI/2.0/\n"
            + "                             http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd\">\n"
            + "  <responseDate>";

    private String xmlProcInst = _PROC_INST + _XMLSTART;
    private String stylesheetLocation = null;
    private Responder m_responder;

    /**
     * Close the Responder at shutdown-time.
     * <p/>
     * This makes a best-effort attempt to properly close any resources
     * (db connections, threads, etc) that are being held.
     */
    public void destroy() {
        try {
            m_responder.close();
        } catch (Exception e) {
            logger.warn("Error trying to close Responder", e);
        }
    }    /**
     * Entry point for handling OAI requests.
     */
    @SuppressWarnings("unchecked")
    public void doGet(HttpServletRequest request,
                      HttpServletResponse response) {

        if (logger.isDebugEnabled()) {
            StringBuffer buf = new StringBuffer();
            buf.append("Started servicing request ( ");
            Map map = request.getParameterMap();
            Iterator nameIter = map.keySet().iterator();
            while (nameIter.hasNext()) {
                String parmName = (String) nameIter.next();
                String[] parmVals = (String[]) map.get(parmName);
                buf.append(parmName + "=" + parmVals[0] + " ");
            }
            buf.append(") from " + request.getRemoteAddr());
            logger.debug(buf.toString());
        }

        String url = null;
        String verb = null;
        String identifier = null;
        String from = null;
        String until = null;
        String metadataPrefix = null;
        String set = null;
        String resumptionToken = null;
        try {
            url = request.getRequestURL().toString();
            verb = request.getParameter("verb");
            if (verb == null) throw new BadVerbException("request did not specify a verb");
            identifier = request.getParameter("identifier");
            from = request.getParameter("from");
            until = request.getParameter("until");
            metadataPrefix = request.getParameter("metadataPrefix");
            set = request.getParameter("set");
            resumptionToken = request.getParameter("resumptionToken");

            // die if any other parameters are given, too
            // this is a bit draconian, but required by the spec nonetheless
            Set argKeys = request.getParameterMap().keySet();
            int argCount = argKeys.size() - 1;
            Iterator names = argKeys.iterator();
            while (names.hasNext()) {
                String n = (String) names.next();
                if (!n.equals("verb")
                        && !n.equals("identifier")
                        && !n.equals("from")
                        && !n.equals("until")
                        && !n.equals("metadataPrefix")
                        && !n.equals("set")
                        && !n.equals("resumptionToken")) {
                    throw new BadArgumentException("unknown argument: " + n);
                }
            }

            ResponseData data = null;
            try {
                if (verb.equals("GetRecord")) {
                    if (argCount != 2) throw new BadArgumentException("two arguments needed, got " + argCount);
                    data = m_responder.getRecord(identifier, metadataPrefix);
                } else if (verb.equals("Identify")) {
                    if (argCount != 0) throw new BadArgumentException("zero arguments needed, got " + argCount);
                    data = m_responder.identify();
                } else if (verb.equals("ListIdentifiers")) {
                    if (identifier != null)
                        throw new BadArgumentException("identifier argument is not valid for this verb");
                    data = m_responder.listIdentifiers(from, until, metadataPrefix, set, resumptionToken);
                } else if (verb.equals("ListMetadataFormats")) {
                    if (argCount > 1) throw new BadArgumentException("one or zero arguments needed, got " + argCount);
                    data = m_responder.listMetadataFormats(identifier);
                } else if (verb.equals("ListRecords")) {
                    if (identifier != null)
                        throw new BadArgumentException("identifier argument is not valid for this verb");
                    data = m_responder.listRecords(from, until, metadataPrefix, set, resumptionToken);
                } else if (verb.equals("ListSets")) {
                    if (argCount > 1) throw new BadArgumentException("one or zero arguments needed, got " + argCount);
                    data = m_responder.listSets(resumptionToken);
                } else {
                    throw new BadVerbException("bad verb: " + verb);
                }
                response.setStatus(HttpServletResponse.SC_OK);
                response.setContentType("text/xml; charset=UTF-8");
                PrintWriter writer = response.getWriter();
                writer.print(getResponseStart(url, verb, identifier, from, until, metadataPrefix, set, resumptionToken, null));
                data.write(response.getWriter());
                writer.println("</OAI-PMH>");
                writer.flush();
                writer.close();
            } finally {
                if (data != null) {
                    try {
                        data.release();
                    } catch (ServerException e) {
                        logger.warn("Could not release response data", e);
                    }
                }
            }
        } catch (ProtocolException e) {
            sendProtocolException(getResponseStart(url,
                            verb,
                            identifier,
                            from,
                            until,
                            metadataPrefix,
                            set,
                            resumptionToken,
                            e),
                    e, response);
        } catch (ServerException e) {
            try {
                logger.warn("OAI Service Error", e);
                response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "OAI Service Error");
            } catch (IOException ioe) {
                logger.warn("Could not send error to client", ioe);
            }
        } catch (Throwable th) {
            try {
                logger.warn("Unexpected Error", th);
                response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Unexpected error");
            } catch (IOException ioe) {
                logger.warn("Could not send error to client", ioe);
            }
        } finally {
            if (logger.isDebugEnabled()) {
                logger.debug("Finished servicing request from " + request.getRemoteAddr());
            }
        }
    }

    public void init() throws ServletException {
        try {
            InputStream propStream = this.getClass().getResourceAsStream("/proai.properties");
            if (propStream == null) {
                throw new IOException("Error loading configuration: /proai.properties not found in classpath");
            }
            Properties props = new Properties();
            props.load(propStream);
            init(props);
        } catch (Exception e) {
            throw new ServletException("Unable to initialize ProviderServlet", e);
        }
    }

    public void init(Properties props) throws ServerException {
        m_responder = new Responder(props);
        //add Stylesheet Instruction to XML if configured
        setStylesheetProperty(props);
    }    private String getResponseStart(String url,
                                    String verb,
                                    String identifier,
                                    String from,
                                    String until,
                                    String metadataPrefix,
                                    String set,
                                    String resumptionToken,
                                    ProtocolException e) { // normally null
        boolean doParams = true;
        if (verb == null) doParams = false;
        if (e != null
                && (e instanceof BadVerbException
                || e instanceof BadArgumentException)) doParams = false;

        StringBuffer buf = new StringBuffer();
        buf.append(appendProcessingInstruction()); // _XML_START replaced for stylesheet instruction 
        buf.append(StreamUtil.nowUTCString());
        buf.append("</responseDate>\n");
        buf.append("  <request");
        if (doParams) {
            appendAttribute("verb", verb, buf);
            appendAttribute("identifier", identifier, buf);
            appendAttribute("from", from, buf);
            appendAttribute("until", until, buf);
            appendAttribute("metadataPrefix", metadataPrefix, buf);
            appendAttribute("set", set, buf);
            appendAttribute("resumptionToken", resumptionToken, buf);
        }
        buf.append(">" + url + "</request>\n");
        return buf.toString();
    }

    /**
     * Method adds Stylesheet Location from proai.properties to the xml-response if available
     *
     * @return
     */
    private void setStylesheetProperty(Properties prop) {
        if (prop.containsKey("proai.stylesheetLocation")) {
            stylesheetLocation = prop.getProperty("proai.stylesheetLocation");
        } else {
            logger.info("No Stylesheet Location given");
        }
    }

    private static void appendAttribute(String name, String value, StringBuffer buf) {
        if (value != null) {
            buf.append(" " + name + "=\"");
            buf.append(StreamUtil.xmlEncode(value));
            buf.append("\"");
        }
    }



    private void sendProtocolException(String responseStart,
                                       ProtocolException e,
                                       HttpServletResponse response) {
        try {
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType("text/xml; charset=UTF-8");
            PrintWriter writer = response.getWriter();
            writer.print(responseStart);
            writer.print("  <error code=\"" + e.getCode() + "\">");
            if (e.getMessage() != null) writer.print(StreamUtil.xmlEncode(e.getMessage()));
            writer.println("</error>");
            writer.println("</OAI-PMH>");
            writer.flush();
            writer.close();
        } catch (Throwable th) {
            logger.warn("Error while sending a protocol exception (" + e.getClass().getName() + ") response", th);
        }
    }



    public void doPost(HttpServletRequest request,
                       HttpServletResponse response) {
        doGet(request, response);
    }


    /**
     * Method adds Stylesheet Instruction to the XML-Processing Instructions if available
     *
     * @return
     */
    private String appendProcessingInstruction() {
        if (stylesheetLocation != null) {
            // add Stylesheet Instruction with a relative Location to XML Head
            xmlProcInst = _PROC_INST + "<?xml-stylesheet type=\"text/xsl\" href=\"" + stylesheetLocation + "\" ?>" + " \n" + _XMLSTART;
            logger.debug("Added Instruction: " + xmlProcInst);
        }
        return xmlProcInst;
    }


}
