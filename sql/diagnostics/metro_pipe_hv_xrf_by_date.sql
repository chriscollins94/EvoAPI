-- Metro Pipe: High Volume rows + latest XRF revisit, for a single Central-time calendar date.
-- Completed datetimes are stored in UTC. Set @QueryDate to the Central date you want and the
-- script converts it to a UTC window, so the WHERE clause compares raw UTC columns (index-friendly)
-- and DST is handled by AT TIME ZONE rather than a fixed -5 hour offset.

DECLARE @QueryDate date = '2026-09-18';   -- <<< Central-time date to report on

DECLARE @CentralTz sysname = 'Central Standard Time';   -- Windows zone name; covers CST/CDT
DECLARE @StartUtc datetime2 = CAST(CAST(@QueryDate AS datetime2)                 AT TIME ZONE @CentralTz AT TIME ZONE 'UTC' AS datetime2);
DECLARE @EndUtc   datetime2 = CAST(CAST(DATEADD(day, 1, @QueryDate) AS datetime2) AT TIME ZONE @CentralTz AT TIME ZONE 'UTC' AS datetime2);

SELECT
    hvbd.hvbd_premisenumber,
    hvbd.hvbd_stanpar,
    hvbd.hvbd_team,
    sr.sr_requestnumber,
    hvbd.hvbd_address,
    hvbd.hvbd_meternumber,
    u.u_firstname + ' ' + u.u_lastname AS Tech,
    CAST(hvbd.hvbd_completeddatetime AT TIME ZONE 'UTC' AT TIME ZONE @CentralTz AS date) AS DateCompleted,
    MAX(CASE WHEN clq.clq_question = 'Before' THEN att.att_filename END) AS Before,
    MAX(CASE WHEN clq.clq_question = 'Before' THEN att.att_latitude END) AS BeforeLatitude,
    MAX(CASE WHEN clq.clq_question = 'Before' THEN att.att_longitude END) AS BeforeLongitude,
    MAX(CASE WHEN clq.clq_question = 'General Condition' THEN xsrcla.xsrcla_answer END) AS GeneralCondition,
    MAX(CASE WHEN clq.clq_question = 'House - Pipe Type' THEN xsrcla.xsrcla_answer END) AS HousePipeType,
    MAX(CASE WHEN clq.clq_question = 'Street - Pipe Type' THEN xsrcla.xsrcla_answer END) AS StreetPipeType,
    MAX(CASE WHEN clq.clq_question = 'Note' THEN xsrcla.xsrcla_answer END) AS Note,

    MAX(CASE WHEN clq.clq_question = 'After' THEN att.att_filename END) AS After,
    MAX(CASE WHEN clq.clq_question = 'After' THEN att.att_latitude END) AS AfterLatitude,
    MAX(CASE WHEN clq.clq_question = 'After' THEN att.att_longitude END) AS AfterLongitude,
    MAX(CASE WHEN clq.clq_question = 'Before' THEN 'https://www.evotrakker.com/ws/api/file/getattachment?att_id=' + CAST(att.att_id AS varchar(50)) + '&att_filename=' + att.att_filename END) AS BeforePhoto,
    MAX(CASE WHEN clq.clq_question = 'After' THEN 'https://www.evotrakker.com/ws/api/file/getattachment?att_id=' + CAST(att.att_id AS varchar(50)) + '&att_filename=' + att.att_filename END) AS AfterPhoto,
    MAX(CASE WHEN clq.clq_question = 'Note - Redox' THEN xsrcla.xsrcla_answer END) AS NoteRedox,

    -- XRF revisit (latest stop loaded for this High Volume row; NULL when the premise is not in any wave)
    MAX(CASE WHEN xrf.xrfbd_id IS NULL THEN 'Not in a wave'
             WHEN xrf.xrfbd_completeddatetime IS NULL THEN 'Pending'
             ELSE 'Submitted' END) AS XrfStatus,
    MAX(xrf.xrfb_filename) AS XrfWave,
    MAX(xrf.xrfbd_team) AS XrfTeam,
    MAX(xrf.xrfbd_result) AS XrfResult,
    MAX(xrf.xrfbd_comment) AS XrfComment,
    MAX(xrf.XrfTech) AS XrfTech,
    MAX(CAST(xrf.xrfbd_completeddatetime AT TIME ZONE 'UTC' AT TIME ZONE @CentralTz AS date)) AS XrfDateCompleted,
    MAX(CAST(xrf.xrfbd_completeddatetime AT TIME ZONE 'UTC' AT TIME ZONE @CentralTz AS datetime)) AS XrfCompletedDateTime,
    MAX(xrf.xrfbd_latitude) AS XrfLatitude,
    MAX(xrf.xrfbd_longitude) AS XrfLongitude,
    MAX(xrf.xrfbd_geoaccuracy) AS XrfAccuracyMeters,
    MAX(CASE WHEN xrf.xrfbd_latitude IS NOT NULL
             THEN 'https://maps.google.com/?q=' + CAST(xrf.xrfbd_latitude AS varchar(20)) + ',' + CAST(xrf.xrfbd_longitude AS varchar(20)) END) AS XrfMapLink,
    MAX(xrf.xrfbd_meternumber) AS XrfListMeterNumber,  -- meter as given on the wave list, informational only

    -- Optional XRF photo taken at submit (attachment row; NULL when the tech did not attach one)
    MAX(xrf.XrfPhotoFilename) AS XrfPhoto,
    MAX(xrf.XrfPhotoLatitude) AS XrfPhotoLatitude,
    MAX(xrf.XrfPhotoLongitude) AS XrfPhotoLongitude,
    MAX(CASE WHEN xrf.xrfbd_att_id IS NOT NULL
             THEN 'https://www.evotrakker.com/ws/api/file/getattachment?att_id=' + CAST(xrf.xrfbd_att_id AS varchar(50))
                  + '&att_filename=' + xrf.XrfPhotoFilename END) AS XrfPhotoLink
FROM
    highvolumebatch hvb
    INNER JOIN highvolumebatchdetail hvbd ON hvb.hvb_id = hvbd.hvb_id
    INNER JOIN servicerequest sr ON hvbd.sr_id = sr.sr_id
    INNER JOIN checklist cl ON hvb.xccc_id = cl.xccc_id
    INNER JOIN checklistquestion clq ON cl.cl_id = clq.cl_id
    LEFT JOIN [user] u ON hvbd.u_id = u.u_id
    LEFT JOIN xrefservicerequestchecklistanswer xsrcla ON clq.clq_id = xsrcla.clq_id AND sr.sr_id = xsrcla.sr_id
    LEFT JOIN attachment att ON xsrcla.att_id = att.att_id
    OUTER APPLY (
        SELECT TOP 1
            d.xrfbd_id, d.xrfbd_team, d.xrfbd_result, d.xrfbd_comment, d.xrfbd_completeddatetime,
            d.xrfbd_latitude, d.xrfbd_longitude, d.xrfbd_geoaccuracy, d.xrfbd_meternumber,
            d.xrfbd_att_id,
            xa.att_filename  AS XrfPhotoFilename,
            xa.att_latitude  AS XrfPhotoLatitude,
            xa.att_longitude AS XrfPhotoLongitude,
            b.xrfb_filename,
            xu.u_firstname + ' ' + xu.u_lastname AS XrfTech
        FROM xrfbatchdetail d
        INNER JOIN xrfbatch b ON b.xrfb_id = d.xrfb_id
        LEFT JOIN [user] xu ON xu.u_id = d.u_id
        LEFT JOIN attachment xa ON xa.att_id = d.xrfbd_att_id   -- PK seek, one row at most
        WHERE d.hvbd_id = hvbd.hvbd_id
        ORDER BY d.xrfbd_completeddatetime DESC, d.xrfbd_id DESC
    ) xrf
WHERE
    (hvbd.hvbd_completeddatetime >= @StartUtc AND hvbd.hvbd_completeddatetime < @EndUtc)
    OR (xrf.xrfbd_completeddatetime >= @StartUtc AND xrf.xrfbd_completeddatetime < @EndUtc)
GROUP BY
    hvbd.hvbd_premisenumber,
    hvbd.hvbd_stanpar,
    hvbd.hvbd_team,
    sr.sr_requestnumber,
    hvbd.hvbd_address,
    hvbd.hvbd_meternumber,
    u.u_firstname + ' ' + u.u_lastname,
    CAST(hvbd.hvbd_completeddatetime AT TIME ZONE 'UTC' AT TIME ZONE @CentralTz AS date)
ORDER BY
    MIN(hvbd.hvbd_order),
    hvbd.hvbd_premisenumber,
    sr.sr_requestnumber;
