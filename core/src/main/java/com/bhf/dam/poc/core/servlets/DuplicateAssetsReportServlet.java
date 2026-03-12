package com.bhf.dam.poc.core.servlets;

import com.google.gson.JsonObject;
import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.api.SlingHttpServletResponse;
import org.apache.sling.api.resource.ResourceResolver;
import org.apache.sling.api.servlets.SlingAllMethodsServlet;
import org.apache.sling.event.jobs.Job;
import org.apache.sling.event.jobs.JobManager;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.servlet.Servlet;
import javax.servlet.ServletException;
import java.io.IOException;
import java.util.*;

@Component(
        immediate = true,
        service = Servlet.class,
        property = {
                "sling.servlet.methods=POST",
                "sling.servlet.resourceTypes=sling/servlet/default",
                "sling.servlet.selectors=bhfdamreport",
                "sling.servlet.extensions=json"
        }
)
public class DuplicateAssetsReportServlet extends SlingAllMethodsServlet {

    private static final Logger logger = LoggerFactory.getLogger(DuplicateAssetsReportServlet.class);

    @Reference
    private JobManager jobManager;

    @Override
    protected void doPost(SlingHttpServletRequest request,
                          SlingHttpServletResponse response)
            throws IOException {

        ResourceResolver resourceResolver = request.getResourceResolver();

        try {
            String jobNodeName = UUID.randomUUID().toString();
            String rootPath = request.getParameter("path");
            String title = request.getParameter("jobTitle");
            String description = request.getParameter("jobDescription");

            if (rootPath == null || rootPath.isEmpty()) {
                rootPath = "/content/dam";
            }

            logger.info("Submitting Duplicate Assets Report job for root path: {}", rootPath);
            Map<String, Object> jobProps = new HashMap<>();
            jobProps.put("rootPath", rootPath);
            jobProps.put("jobTitle", title);
            jobProps.put("jobDescription", description);
            jobProps.put("jobNodePath", "/var/dam/reports/" + jobNodeName);

            String[] customProperties = request.getParameterValues("customproperties");

            if (customProperties == null || customProperties.length == 0) {
                customProperties = new String[0];
            }

            List<String> customProps = new ArrayList<>(Arrays.asList(customProperties));

            List<String> columns = new ArrayList<>(Arrays.asList(request.getParameterValues("column")));

            jobProps.put("reportColumns", columns.toArray(new String[0]));
            jobProps.put("customProperties", customProps.toArray(new String[0]));

            Node jobNode = createJobNode(resourceResolver, jobNodeName, request, columns, rootPath, title, description);
            Calendar createdTime = Calendar.getInstance();
            createdTime.setTimeInMillis(createdTime.getTimeInMillis());

            Job job = jobManager.addJob(
                    "com/bhfdampoc/dam/report/duplicate-assets",
                    jobProps
            );

            jobNode.setProperty("jobId", job.getId());
            jobNode.setProperty("jobStatus", "running");
            jobNode.setProperty("jcr:created", createdTime);

            response.setContentType("application/json");
            response.setStatus(SlingHttpServletResponse.SC_OK);

            JsonObject jsonResponse = new JsonObject();
            jsonResponse.addProperty("jobId", job.getId());
            jsonResponse.addProperty("message", "Job submitted successfully");
            jsonResponse.addProperty("success", true);
            response.getWriter().write(jsonResponse.toString());

            resourceResolver.commit();

        } catch (Exception e) {
            response.setStatus(SlingHttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            response.getWriter().write("Error submitting job: " + e.getMessage());
        }

    }

    private Node createJobNode(ResourceResolver resourceResolver, String jobNodeName, SlingHttpServletRequest request, List<String> columns, String path, String title, String desc) throws RepositoryException {
        Session session = resourceResolver.adaptTo(Session.class);
        Node jobNode, baseNode;
        String baseNodePath = "/var/dam/reports";

        if (resourceResolver.getResource(baseNodePath) == null) {
            jobNode = session.getNode("/var/dam");
            baseNode = jobNode.addNode("reports", "sling:Folder");
        } else {
            baseNode = session.getNode(baseNodePath);
        }

        jobNode = baseNode.addNode(jobNodeName.replaceAll("/", "-"), "nt:unstructured");
        jobNode.setProperty("reportType", request.getParameter("dam-asset-report-type"));
        jobNode.setProperty("rootPath", path);
        jobNode.setProperty("jobTitle", title);
        jobNode.setProperty("jobDescription", desc);
        jobNode.setProperty("reportColumns", columns.toArray(new String[0]));
        jobNode.setProperty("reportCsvColumns", columns.stream().map(String::toLowerCase).toArray(String[]::new));

        session.save();

        return jobNode;

    }
}