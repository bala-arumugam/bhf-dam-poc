package com.bhf.dam.poc.core.filters;

import com.adobe.granite.ui.components.Config;
import com.adobe.granite.ui.components.ExpressionResolver;
import com.adobe.granite.ui.components.PagingIterator;
import com.adobe.granite.ui.components.ds.AbstractDataSource;
import com.adobe.granite.ui.components.ds.DataSource;
import org.apache.commons.collections.Transformer;
import org.apache.commons.collections.iterators.TransformIterator;
import org.apache.commons.lang3.StringUtils;
import org.apache.sling.api.SlingHttpServletRequest;
import org.apache.sling.api.resource.Resource;
import org.apache.sling.api.resource.ResourceWrapper;
import org.apache.sling.api.resource.ValueMap;
import org.apache.sling.engine.EngineConstants;
import org.osgi.framework.Constants;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
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
            "/apps/dam/assetreports/duplicateassetsreport";

    private static final String OTB_REPORTS_PATH =
            "dam/content/reports/availablereports";

    @Reference
    private ExpressionResolver expressionResolver;

    @Override
    public void init(FilterConfig filterConfig) {
        logger.info("AddDuplicateAssetsReportFilter initialized");
    }

    public void doFilter(ServletRequest request, ServletResponse response,
                         FilterChain filterChain) throws IOException, ServletException {

        SlingHttpServletRequest slingRequest = (SlingHttpServletRequest) request;
        logger.debug("Filter invoked for path: {}", slingRequest.getRequestPathInfo());

        Resource resource = slingRequest.getResource();
        Resource repConfigRes = resource.getChild(Config.DATASOURCE);

        filterChain.doFilter(request, response);

        if(repConfigRes == null){
            return;
        }

        ValueMap repConfigVM = repConfigRes.getValueMap();
        String reportPath = repConfigVM.get("reportPath", "");
        String itemRT = repConfigVM.get("itemResourceType", "");

        if(!OTB_REPORTS_PATH.equals(reportPath) || StringUtils.isEmpty(itemRT)){
            return;
        }

        AbstractDataSource ds = (AbstractDataSource)request.getAttribute(DataSource.class.getName());

        final List<Resource> sortedList = new ArrayList<Resource>();
        Iterator<Resource> items = ds.iterator();

        while(items.hasNext()){
            sortedList.add(items.next());
        }

        sortedList.add(slingRequest.getResourceResolver().getResource(CUSTOM_REPORT_PATH));

        ds = new AbstractDataSource() {
            public Iterator<Resource> iterator() {
                return new TransformIterator(new PagingIterator(sortedList.iterator(), 0, 100), new Transformer() {
                    public Object transform(Object o) {
                        final Resource r = (Resource)o;
                        return new ResourceWrapper(r) {
                            public String getResourceType() {
                                return itemRT;
                            }
                        };
                    }
                });
            }
        };

        request.setAttribute(DataSource.class.getName(), ds);
    }

    @Override
    public void destroy() {

    }
}