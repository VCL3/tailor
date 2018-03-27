/**
 * Created by wliu on 12/13/17.
 */
package com.intrence.datapipeline.tailor.resources;

import com.google.inject.Inject;
import com.intrence.core.persistence.dao.ProductDao;
import com.intrence.datapipeline.tailor.exception.InternalServerException;
import com.intrence.datapipeline.tailor.net.FetchRequest;
import com.intrence.datapipeline.tailor.net.RequestResponse;
import com.intrence.datapipeline.tailor.net.WebFetcher;
import com.intrence.datapipeline.tailor.parser.BaseParser;
import com.intrence.datapipeline.tailor.parser.ParserFactory;
import com.intrence.models.model.DataPoint;
import org.apache.http.client.methods.HttpGet;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Set;

@Path("/tailor/v1/product")
@Produces(MediaType.APPLICATION_JSON)
public class FetchProductResource {

    private final WebFetcher webFetcher;
    private final ProductDao productDao;

    @Inject
    public FetchProductResource(WebFetcher webFetcher, ProductDao productDao) {
        this.webFetcher = webFetcher;
        this.productDao = productDao;
    }

    @GET
    @Path("fetch")
    public Response fetchProductForUrl(@QueryParam("source") String source,
                                       @QueryParam("url") String productUrl) throws Exception {

        FetchRequest fetchRequest = new FetchRequest.Builder()
                .workRequest(productUrl)
                .priority(0)
                .methodType(HttpGet.METHOD_NAME)
                .build();

        RequestResponse requestResponse = this.webFetcher.getResponse(source, fetchRequest);

        if (requestResponse.isSuccess()) {

            // Get the proper parser
            BaseParser parser = (BaseParser) ParserFactory.createParser(source, requestResponse.getResponse(), requestResponse.getRequest());

            Set<DataPoint> dataPoints = parser.extractEntities();
            if (dataPoints != null && dataPoints.size() > 0) {
                for (DataPoint dataPoint : dataPoints) {
                    productDao.createProduct(dataPoint.getProduct());
                    return Response.ok(dataPoint.getProduct().toJson()).build();
                }
            } else {
                return Response.ok("No Data Point").build();
            }
        } else {
            throw new InternalServerException("Fail to fetch for product");
        }

        return Response.ok("Finished").build();
    }

}
