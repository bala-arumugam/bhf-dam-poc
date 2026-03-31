package com.bhf.dam.poc.core.jobs;

import com.bhf.dam.poc.core.beans.ColumnDefinition;
import com.day.cq.search.PredicateGroup;
import com.day.cq.search.Query;
import com.day.cq.search.QueryBuilder;
import com.day.cq.search.result.Hit;
import com.day.cq.search.result.SearchResult;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.resource.ResourceResolverFactory;
import org.apache.sling.event.jobs.Job;
import org.apache.sling.event.jobs.consumer.JobConsumer;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

@Component(
        immediate = true,
        service = JobConsumer.class,
        property = {
                JobConsumer.PROPERTY_TOPICS + "=com/bhfdampoc/dam/report/duplicate-assets"
        }
)
public class DuplicateAssetsReportJobConsumer implements JobConsumer {

    private static final Logger logger = LoggerFactory.getLogger(DuplicateAssetsReportJobConsumer.class);

    @Reference
    private ResourceResolverFactory resolverFactory;

    @Reference
    private QueryBuilder queryBuilder;

    @Override
    public JobResult process(Job job) {

        String reportPath = job.getProperty("jobNodePath", String.class);

        Node reportNode = null;

        Map<String, Object> authInfo = Map.of(
                ResourceResolverFactory.SUBSERVICE, "report-service"
        );
        File csvFile = null;

        try (ResourceResolver resolver =
                     resolverFactory.getServiceResourceResolver(authInfo)) {

            Session session = resolver.adaptTo(Session.class);

            reportNode = session.getNode(reportPath);

            String reportTitle = reportNode.getProperty("jobTitle").getString();

            String rootPath = reportNode.getProperty("path").getString();

            String reportType = reportNode.getProperty("reportType").getString();

            if (!reportType.equals("duplicateassetsreport")) {
                return JobResult.OK;
            }

            String[] columns = job.getProperty("reportColumns", String[].class);

            String[] customProperties = job.getProperty("customProperties", String[].class);

            if (columns == null || columns.length == 0) {
                columns = new String[0];
            }

            if (customProperties == null || customProperties.length == 0) {
                customProperties = new String[0];
            }

            List<ColumnDefinition> columnDefinitions = buildColumnDefinitions(columns, customProperties);


            List<Node> duplicateAssets =
                    findDuplicateAssets(session, rootPath);

            csvFile = generateCsv(
                    columnDefinitions,
                    duplicateAssets
            );

            saveCsvAsBinary(reportNode, session, csvFile, reportTitle);

            reportNode.setProperty("jobStatus", "completed");
            session.save();

            //return JobResult.OK;

        } catch (Exception e) {
            logger.error("Error processing Duplicate Assets Report job", e);
            try {
                reportNode.setProperty("jobStatus", "failed");
            } catch (RepositoryException ex) {
                throw new RuntimeException(ex);
            }
            return JobResult.FAILED;
        } finally {
            if (csvFile != null && csvFile.exists()) {
                csvFile.delete();
            }
        }
        return JobResult.OK;
    }

    private List<ColumnDefinition> buildColumnDefinitions(String[] columns,
                                                          String[] customProps) {

        List<ColumnDefinition> defs = new ArrayList<>();

        int customIndex = 0;

        for (String col : columns) {

            if (isDefaultColumn(col)) {

                defs.add(new ColumnDefinition(col, ""));

            } else {

                String prop = "";

                if (customProps != null && customIndex < customProps.length) {
                    prop = customProps[customIndex++];
                }

                defs.add(new ColumnDefinition(col, prop));
            }
        }

        return defs;
    }

    private boolean isDefaultColumn(String col) {

        return Arrays.asList("title", "path", "type", "size", "added", "modified", "expires", "published", "brand portal published")
                .contains(col.toLowerCase());
    }


    // ---------------------------------------------------------
//       Find duplicate assets
// ---------------------------------------------------------
    private List<Node> findDuplicateAssets(Session session,
                                           String rootPath)
            throws RepositoryException {

        List<Node> duplicates = new ArrayList<>();

        Map<String, String> predicates = new HashMap<>();
        predicates.put("type", "dam:Asset");
        predicates.put("path", rootPath);
        predicates.put("property", "jcr:content/metadata/dam:isDuplicate");
        predicates.put("property.value", "true");
        predicates.put("p.limit", "-1");

        Query query = queryBuilder.createQuery(
                PredicateGroup.create(predicates), session);

        SearchResult result = query.getResult();

        for (Hit hit : result.getHits()) {
            duplicates.add(hit.getNode());
        }

        return duplicates;
    }

    // ---------------------------------------------------------
//      Generate CSV dynamically
// ---------------------------------------------------------
    private File generateCsv(List<ColumnDefinition> columns,
                             List<Node> assets)
            throws Exception {

        File file = File.createTempFile("duplicate-assets-", ".csv");

        try (BufferedWriter writer =
                     new BufferedWriter(
                             new OutputStreamWriter(
                                     new FileOutputStream(file),
                                     StandardCharsets.UTF_8))) {

            writer.write(columns.stream()
                    .map(ColumnDefinition::getHeader)
                    .map(String::toUpperCase)
                    .collect(Collectors.joining(",")));

            writer.newLine();

// ---- Write Rows ----
            for (Node asset : assets) {

                List<String> rowValues = new ArrayList<>();

                for (ColumnDefinition column : columns) {

                    rowValues.add(escapeCsv(resolveValue(asset, column)));
                }

                writer.write(String.join(",", rowValues));
                writer.newLine();
            }
        }

        return file;
    }


    public String resolveValue(Node asset, ColumnDefinition def) {

        if (Objects.equals(def.getPropertyPath(), ""))
            return resolveDefaultColumn(asset, def.getHeader());

        return resolveByJcrPath(asset, def.getPropertyPath());
    }

    // ---------------------------------------------------------
//       Resolve property dynamically
// ---------------------------------------------------------
    private String resolveDefaultColumn(Node asset, String column) {

        try {
            Node contentNode = asset.getNode("jcr:content");
            Node metadataNode = asset.getNode("jcr:content").getNode("metadata");

            switch (column) {
                case "title":
                    return metadataNode.hasProperty("dc:title")
                            ? metadataNode.getProperty("dc:title").getString()
                            : asset.getName();

                case "path":
                    return asset.getPath();

                case "type":
                    return metadataNode.hasProperty("dc:format")
                            ? metadataNode.getProperty("dc:format").getString()
                            : "";

                case "size":
                    return metadataNode.hasProperty("dam:size")
                            ? metadataNode.getProperty("dam:size").getString()
                            : "";

                case "added":
                    if (asset.hasProperty("jcr:created")) {
                        Calendar created = asset.getProperty("jcr:created").getDate();
                        return created.getTime().toString();
                    }
                    break;
                case "modified":
                    if (contentNode.hasProperty("jcr:lastModified")) {
                        Calendar lastModified = contentNode.getProperty("jcr:lastModified").getDate();
                        return lastModified.getTime().toString();
                    }
                    break;
                case "expires":
                    if (metadataNode.hasProperty("prism:expirationDate")) {
                        Calendar expiryDate = metadataNode.getProperty("prism:expirationDate").getDate();
                        return expiryDate.getTime().toString();
                    }
                    break;
                case "published":
                    if (contentNode.hasProperty("cq:lastReplicated")) {
                        Calendar published = contentNode.getProperty("cq:lastReplicated").getDate();
                        return published.getTime().toString();
                    }
                    break;
                default:
                    return "";
            }

        } catch (Exception e) {
            return "";
        }
        return "";
    }

    private String resolveByJcrPath(Node asset, String propertyPath) {

        try {

            Node current = asset;

            String[] parts = propertyPath.split("/");

            for (int i = 0; i < parts.length - 1; i++) {
                if (!parts[i].isEmpty()) {
                    current = current.getNode(parts[i]);
                }
            }

            String propName = parts[parts.length - 1];

            if (!current.hasProperty(propName)) {
                return "";
            }

            Property prop = current.getProperty(propName);

            if (prop.isMultiple()) {
                return Arrays.stream(prop.getValues())
                        .map(v -> {
                            try {
                                return v.getString();
                            } catch (Exception e) {
                                return "";
                            }
                        })
                        .collect(Collectors.joining("|"));
            }

            return prop.getString();

        } catch (Exception e) {
            return "";
        }
    }


    // ---------------------------------------------------------
//  Save CSV as nt:file under report node
// ---------------------------------------------------------
    private void saveCsvAsBinary(Node reportNode,
                                 Session session,
                                 File file, String title)
            throws Exception {

        Node fileNode = reportNode.addNode(
                title + ".csv",
                "nt:file"
        );

        Node contentNode = fileNode.addNode(
                "jcr:content",
                "nt:resource"
        );

        Binary binary = session.getValueFactory()
                .createBinary(new FileInputStream(file));

        contentNode.setProperty("jcr:data", binary);
        contentNode.setProperty("jcr:mimeType", "text/csv");
        contentNode.setProperty("jcr:lastModified",
                Calendar.getInstance());

        session.save();
    }

    // ---------------------------------------------------------
//  CSV escape
// ---------------------------------------------------------
    private String escapeCsv(String value) {

        if (value == null) return "";

        if (value.contains(",") || value.contains("\"")) {
            value = value.replace("\"", "\"\"");
            return "\"" + value + "\"";
        }

        return value;
    }
}