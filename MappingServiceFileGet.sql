IF EXISTS(SELECT 1 FROM sysobjects WHERE name ='MappingServiceFileGet' AND xtype ='P')
DROP PROC MappingServiceFileGet
GO

CREATE PROCEDURE

[dbo].[MappingServiceFileGet]

AS

	-- This SP returns a file that is in StatusId 1. The service will grab that file and it will start loading its data into the DB (StatusId 2)

	-- We first discard files automatically (those that are PNG images, for example)
	EXEC dbo.MSProcessAutoDiscardFiles


	-- we need to determine files with StatusId = 7 that are "big"

	DECLARE @BigFilesReprocessingTimeBegin INTEGER
	DECLARE @BigFilesReprocessingTimeEnd INTEGER
	
	SELECT @BigFilesReprocessingTimeBegin = ConfigValue 
	FROM MSConfig WITH(NOLOCK)
	WHERE ConfigName = 'BigFilesReprocessingTimeBegin'
		
	SELECT @BigFilesReprocessingTimeEnd = ConfigValue 
	FROM MSConfig WITH(NOLOCK)
	WHERE ConfigName = 'BigFilesReprocessingTimeEnd'
	
	DECLARE @BigFileProcessing INTEGER
	
	SET @BigFileProcessing = 0

	DECLARE @FileId INTEGER

	IF (@BigFilesReprocessingTimeBegin <> -1 AND @BigFilesReprocessingTimeEnd <> -1)
		IF (DATEPART(HOUR, GETDATE()) BETWEEN @BigFilesReprocessingTimeBegin AND @BigFilesReprocessingTimeEnd - 1)
			BEGIN	
				
				SELECT @FileId = ISNULL(MIN(FileId), -1) FROM MSFile AS f WITH(NOLOCK)
				WHERE EXISTS
				(
					SELECT 1 from MSEventLog AS e WITH(NOLOCK)
					WHERE ExceptionMessage LIKE '%timeout%'
					AND f.FileId = e.FileId
					GROUP BY FileId
				)
				AND StatusId = 7
				AND NOT EXISTS
				(
					SELECT 1 FROM MSReprocessedFiles AS r WITH(NOLOCK)
					WHERE r.FileId = f.FileId
				)		


				IF @FileId <> -1
					BEGIN
					
						INSERT INTO MSReprocessedFiles (FileId, ReprocessedDate)
						SELECT @FileId, GETDATE()

						EXEC [dbo].[MSReprocessFile] @FileId			
					
						SET @BigFileProcessing = 1
					
					END
			
			END




-- this SP will return the first FileId with Status = 1 (not loaded yet)

	
	DECLARE @EmailId INTEGER
	
	IF @BigFileProcessing = 0
		BEGIN
			
			-- we first grab files with Priority = 1
			SELECT @FileId = ISNULL(MIN(FileId), 0) FROM MSFile WITH(NOLOCK)
			WHERE StatusId = 1
			AND Priority = 1	
			
			-- if no file with priority is found, then we grab the first older file
			IF ISNULL(@FileId, 0) = 0
				SELECT @FileId = ISNULL(MIN(FileId), 0) FROM MSFile WITH(NOLOCK)
				WHERE StatusId = 1
			
			SELECT @EmailId = EmailId FROM MSFile WITH(NOLOCK)
			WHERE FileId = @FileId
			
		END
	ELSE -- @BigFileProcessing = 1
		BEGIN
		
			SELECT @EmailId = EmailId FROM MSFile WITH(NOLOCK)
			WHERE FileId = @FileId		
		
		END	
	

	DECLARE @TodayTimeId INTEGER
	SELECT @TodayTimeId = dbo.GetComscoreTimeID (GETDATE())	
	
	
	IF @FileId > 0
		BEGIN
		
			SELECT
				f.FileId
				, f.EmailId
				, ISNULL(e.ReceivedTimeId, @TodayTimeId) AS 'ReceivedTimeId'
				, f.StatusId
				, f.FileName
				, f.FullPath
				, f.Timestamp
				, f.ProcessedDate
				, f.FileSizeBytes
				, @BigFileProcessing AS 'BigFileProcessing'
				, f.Priority
			FROM MSFile AS f WITH(NOLOCK)
			LEFT JOIN MSEmail AS e WITH (NOLOCK) ON f.EmailId = e.EmailId
			WHERE f.FileId = @FileId
						
			UPDATE MSFile -- This FileId status gets updated to 2 (Loading Raw Data)
			SET StatusId = 2
			WHERE FileId = @FileId
		
		END
	ELSE
		SELECT
		0 AS FileId
		, 0 AS EmailId
		, 0 AS ReceivedTimeId
		, 0 AS StatusId
		, '' AS 'FileName'
		, '' AS 'FullPath'
		, NULL AS Timestamp
		, NULL AS ProcessedDate
		, 0 AS FileSizeBytes
		, 0 AS 'BigFileProcessing'
		, 0 AS 'Priority'

GO