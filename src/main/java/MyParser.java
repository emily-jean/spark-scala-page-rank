import java.io.StringReader;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.hadoop.util.StringUtils;
import org.xml.sax.*;
import org.xml.sax.helpers.DefaultHandler;

/**
 *  Decompresses bz2 file and parses Wikipages on each line.
 *  Give by CS6240 on BlackBoard NEU */
public class MyParser {
    private static Pattern namePattern;
    private static Pattern linkPattern;
    static {
        // Keep only html pages not containing tilde (~).
        namePattern = Pattern.compile("^([^~]+)$");
        // Keep only html filenames ending relative paths and not containing tilde (~).
        linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
    }

    public static String readXML(String args) throws SAXException, ParserConfigurationException {
            // Configure parser.
            SAXParserFactory spf = SAXParserFactory.newInstance();
            spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
            SAXParser saxParser = spf.newSAXParser();
            XMLReader xmlReader = saxParser.getXMLReader();
            // Parser fills this list with linked page names.
            List<String> linkPageNames = new ArrayList<>();
            xmlReader.setContentHandler(new WikiParser(linkPageNames));
            int delimLoc = args.indexOf(':');
            String pageName = args.substring(0, delimLoc);
            String html = args.substring(delimLoc + 1);
            html = html.replaceAll(" & ","&amp;");
            Matcher matcher = namePattern.matcher(pageName);
            if (!matcher.find()) {
                // Skip this html file, name contains (~).
                return "bad";
            }

            // Parse page and fill list of linked pages.
            linkPageNames.clear();
            try {
                xmlReader.parse(new InputSource(new StringReader(html)));
            } catch (Exception e) {
                // Discard ill-formatted pages.
                return "bad";
            }

            // removes page if page references itself
            if(linkPageNames.contains(pageName)){
                int index= linkPageNames.indexOf(pageName);
                linkPageNames.remove(index);
            }

            return pageName +"dummy"+ toStringValue(linkPageNames);

    }

    private static String toStringValue(List<String> linkPageNames) {
        return StringUtils.join("~", linkPageNames);
    }

    /** Parses a Wikipage, finding links inside bodyContent div element. */
    private static class WikiParser extends DefaultHandler {
        /** List of linked pages; filled by parser. */
        private List<String> linkPageNames;
        /** Nesting depth inside bodyContent div element. */
        private int count = 0;

        public WikiParser(List<String> linkPageNames) {
            super();
            this.linkPageNames = linkPageNames;
        }

        @Override
        public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
            super.startElement(uri, localName, qName, attributes);
            if ("div".equalsIgnoreCase(qName) && "bodyContent".equalsIgnoreCase(attributes.getValue("id")) && count == 0) {
                // Beginning of bodyContent div element.
                count = 1;
            } else if (count > 0 && "a".equalsIgnoreCase(qName)) {
                // Anchor tag inside bodyContent div element.
                count++;
                String link = attributes.getValue("href");
                if (link == null) {
                    return;
                }
                try {
                    // Decode escaped characters in URL.
                    link = URLDecoder.decode(link, "UTF-8");            // Getting values within href with special chars
                } catch (Exception e) {
                    // Wiki-weirdness; use link as is.
                }
                // Keep only html filenames ending relative paths and not containing tilde (~).
                Matcher matcher = linkPattern.matcher(link);
                // Removes duplicate links in outlinks
                if (matcher.find()) {
                    if(!linkPageNames.contains(matcher.group(1)))
                        linkPageNames.add(matcher.group(1));
                }
            } else if (count > 0) {
                // Other element inside bodyContent div.
                count++;
            }
        }

        @Override
        public void endElement(String uri, String localName, String qName) throws SAXException {
            super.endElement(uri, localName, qName);
            if (count > 0) {
                // End of element inside bodyContent div.
                count--;
            }
        }
    }
}