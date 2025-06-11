@Repository
public interface JpaIncomingTransferRepository extends JpaCommonRepositoryPart<JpaTransferPart.ID,JpaIncomingTransfer> {
    @Query
    Optional<JpaIncomingTransfer> findByExtId(String extId);

    @Query(value = "select t from JpaIncomingTransfer t  where extId = :extId and id.part>=:partFrom and id.part<=:partTo")
    Optional<JpaIncomingTransfer> findByExtId(String extId,Integer partFrom,Integer partTo);

    @Query(value = "select t from JpaIncomingTransfer t  where extId = :extId and part = :part")
    Optional<JpaIncomingTransfer> findByExtId(String extId, Integer part);

    @Query(nativeQuery = true, value = "select ID,PART from INCOMING_TRANSFER  where not IS_SEND_TO_KAFKA and CREATE_DATE<:date and PART>=:partFrom and PART<=:partTo LIMIT :limit")
    List<Object[]> findTransferToSendKafka(@Param("limit") Long limit,@Param("date") Date date, @Param("partFrom") Integer partFrom, @Param("partTo") Integer partTo);

    @Query(value = "select ID,PART from INCOMING_TRANSFER  WHERE status_value = :transferStatusEnum and next_date_for_retry <= :date and PART>=:partFrom and PART<=:partTo LIMIT :limit", nativeQuery = true)
    List<Object[]> findTransfersByStatusWithLimit(@Param("limit") Long limit, @Param("transferStatusEnum") String transferStatusEnum, Date date, @Param("partFrom") Integer partFrom, @Param("partTo") Integer partTo);

    @Query(value =
            "select A.ID, A.PART from INCOMING_TRANSFER A  where A.part=:partFrom and  A.status_value in (:transferStatuses) and A.CREATE_DATE <= :creationDate and A.ID not in (select b.ID from TASK_JOURNAL B where B.part=:partFrom and B.TASK_TYPE = :taskType)" +
                    " UNION ALL" +
                    " select A.ID, A.PART from INCOMING_TRANSFER A  where A.part=:partTo and  A.status_value in (:transferStatuses) and A.CREATE_DATE <= :creationDate and A.ID not in (select b.ID from TASK_JOURNAL B where B.part=:partTo and B.TASK_TYPE = :taskType)" +
                    " LIMIT :limit"
            , nativeQuery = true)

    List<Object[]> findAccidentTransfersWithLimit(@Param("limit") Long limit, @Param("transferStatuses") List<String> transferStatuses, @Param("taskType") String taskType, Date creationDate, @Param("partFrom") Integer partFrom, @Param("partTo") Integer partTo);


    @Query(value = "select nextval(:seq) from generate_series(1,:cnt);", nativeQuery = true)
    List<Long> nextValSeq(@Param("seq") String seq, @Param("cnt") int cnt);

    @Query(value = "select t from JpaIncomingTransfer t  where suit = :suit and part = :part")
    Optional<JpaIncomingTransfer> findBySuit(String suit, Integer part);


}
