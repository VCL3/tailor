/**
 * Created by wliu on 12/18/17.
 */
package com.intrence.datapipeline.tailor.resources;

import com.google.inject.Inject;
import com.intrence.core.elasticsearch.ElasticSearchService;
import com.intrence.models.model.Product;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/tailor/v1/product")
@Produces(MediaType.APPLICATION_JSON)
public class IndexProductResource {

    private final ElasticSearchService elasticSearchService;

    @Inject
    public IndexProductResource(ElasticSearchService elasticSearchService) {
        this.elasticSearchService = elasticSearchService;
    }

    @POST
    @Path("index")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response indexProductGivenJsonToElasticSearch(@QueryParam("source") String source,
                                                         @QueryParam("url") String productUrl) throws Exception {

        return Response.ok().build();
    }

    @GET
    @Path("index/{id}")
    public Response readProductByIdFromElasticSearch(@PathParam("id") String id,
                                                     @QueryParam("index") String index,
                                                     @QueryParam("type") String type) throws Exception {

        String res = this.elasticSearchService.getDocument(index, type, id).getSourceAsString();
        return Response.ok(Product.fromJson(res)).build();
    }

    @GET
    @Path("index/list")
    public Response readProductByIdFromElasticSearch() throws Exception {
        String[] indices = this.elasticSearchService.getAllIndices();
        return Response.ok(indices).build();
    }

}
