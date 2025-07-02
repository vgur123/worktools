@RequiredArgsConstructor
@Configuration
public class Controller {
    private final UfsFlow ufsFlow;

    @Bean
    RouterFunction<ServerResponse> router() {
        return RouterFunctions.route()
                .POST("/p2p/sbp/rest/v1", request -> flow(request, ufsFlow, TransferRq.class))
                .build();
    }

    private <RQ, RS> Mono<ServerResponse> flow(ServerRequest request, SbpRouterCommonFlow<RQ, RS> flowService, Class<RQ> rqClass) {
        return request.bodyToMono(rqClass).map(rq -> new RequestWithParam<>(rq, getParam(request)))
                .flatMap(flowService)
                .flatMap(rs -> ServerResponse.ok()
                        .headers(headers -> headers.addAll(setParam(request)))
                        .bodyValue(rs));
    }

    private SbpRouterRequestParam getParam(ServerRequest request) {
        ServerRequest.Headers headers = request.headers();
        SbpRouterRequestParam param = new SbpRouterRequestParam();
        param.setXRequestId(headers.firstHeader("x-synapse-rquid"));
        return param;
    }

    private HttpHeaders setParam(ServerRequest request) {
        ServerRequest.Headers requestHeaders = request.headers();
        HttpHeaders responseHeaders = new HttpHeaders();
        return responseHeaders;
    }

}
