public class AsynkServiceImpl inplements AsynkService {
  private AsynkProperties asynkProperties;
  private Scheduler scheduler;
  
  @Override
  public <T> Mono<T> backProcess(Mono<T> mono){
    return mono.publishOn(scheduler);
  }
  
   @Override
   public void schedule(Runnable task){
    scheduler.schedule(task);
   }
   
   @PostConstruct
   private void init(){
    scheduler = Schedulers.newBoundedElastic(asynkProperties.getThredCap(), ...);
   }
}
