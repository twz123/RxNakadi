package org.zalando.nakadilib.domain;

import com.google.common.base.MoreObjects;

public class PublishingProblem {

    private String publishingStatus;
    private String step;
    private String detail;
    private String eid;

    @Override
    public String toString() {
        return
            MoreObjects.toStringHelper(this)                      //
                       .omitNullValues()                          //
                       .add("publishingStatus", publishingStatus) //
                       .add("step", step)                         //
                       .add("detail", detail)                     //
                       .add("eid", eid)                           //
                       .toString();
    }

    public String getPublishingStatus() {
        return publishingStatus;
    }

    public String getStep() {
        return step;
    }

    public String getDetail() {
        return detail;
    }

    public String getEid() {
        return eid;
    }
}
