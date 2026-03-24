(function ($, $document) {
    "use strict";

    var _ = window._,
        initialized = false,
        REPORT_TYPE_DETAIL = "Duplicate Assets",
        REPORT_LIST_PAGE = "/mnt/overlay/dam/gui/content/reports/reportlist.html";

    if (!isReportListPage()) {
        return;
    }

    init();

    function init(){
        if(initialized){
            return;
        }

        initialized = true;

        // Wait for the page to load, then fix any report entries with missing titles
        $(document).one("foundation-contentloaded", function(e){
            $("[value='null']").html(REPORT_TYPE_DETAIL);
        });
    }

    function isReportListPage() {
        return (window.location.pathname.indexOf(REPORT_LIST_PAGE) >= 0);
    }
}(jQuery, jQuery(document)));