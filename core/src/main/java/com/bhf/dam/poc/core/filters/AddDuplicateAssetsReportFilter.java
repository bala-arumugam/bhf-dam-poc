package com.bhf.dam.poc.core.filters;

import com.adobe.granite.ui.components.Config;
import com.adobe.granite.ui.components.PagingIterator;
import com.adobe.granite.ui.components.ds.AbstractDataSource;
import com.adobe.granite.ui.components.ds.DataSource;
import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ValueMap;
import org.apache.sling.engine.EngineConstants;
import org.osgi.framework.Constants;
import org.osgi.service.component.annotations.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Component(
        service = Filter.class,
        immediate = true,
        name = "Duplicate Assets Filter",
        property = {
                Constants.SERVICE_RANKING + ":Integer=-99",
                EngineConstants.SLING_FILTER_SCOPE + "=" + EngineConstants.FILTER_SCOPE_INCLUDE,
                EngineConstants.SLING_FILTER_RESOURCETYPES + "=" + "dam/gui/coral/components/commons/ui/shell/datasources/reportlistdatasource",
        }
)
public class AddDuplicateAssetsReportFilter implements Filter {

    private static final Logger logger = LoggerFactory.getLogger(AddDuplicateAssetsReportFilter.class);

    private static final String CUSTOM_REPORT_PATH =
            "/apps/dam/content/reports/availablereports/duplicate-assets-report";

    private static final String OTB_REPORTS_PATH =
            "dam/content/reports/availablereports";

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        logger.info("AddDuplicateAssetsReportFilter initialized");
    }

    public void doFilter(ServletRequest req, ServletResponse res,
                         FilterChain chain) throws IOException, ServletException {

        SlingHttpServletRequest slingRequest = (SlingHttpServletRequest) req;
        logger.debug("Filter invoked for path: {}", slingRequest.getRequestPathInfo());

        Resource ds = slingRequest.getResource().getChild(Config.DATASOURCE);

        chain.doFilter(req, res);

        if (ds == null) return;

        ValueMap vm = ds.getValueMap();
        String reportPath = vm.get("reportPath", "");

        if (!OTB_REPORTS_PATH.equals(reportPath)) {
            return;
        }

        AbstractDataSource origDs =
                (AbstractDataSource) slingRequest.getAttribute(DataSource.class.getName());

        if (origDs == null) {
            logger.warn("No DataSource found in request");
            return;
        }

        logger.debug("Found DataSource, processing reports");

        List<Resource> allReports = new ArrayList<>();
        origDs.iterator().forEachRemaining(allReports::add);

        Resource custom = slingRequest.getResourceResolver()
                .getResource(CUSTOM_REPORT_PATH);

        allReports.add(custom);

        final Iterator<Resource> it = allReports.iterator();

        AbstractDataSource newDs = new AbstractDataSource() {
            public Iterator<Resource> iterator() {
                return new PagingIterator(it, 0, 100);
            }
        };

        req.setAttribute(DataSource.class.getName(), newDs);
    }

    @Override
    public void destroy() {

    }
}