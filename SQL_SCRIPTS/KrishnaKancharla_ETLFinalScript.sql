/***************************************************************************
ETL Final Project: ETLFinal
Dev: KKancharla
Date:2022/02/27
Desc: This is a ETL script to flush and fill tables for the Patient and DoctorsSchedule Databases.
ChangeLog: (Who, When, What) 
  KKancharla, 2/27/2022, Updated code to include logging and transaction handling
*****************************************************************************************/
--USE [master]
--GO
--If Exists (Select * from Sysdatabases Where Name = 'DWClinicReportDataKrishnaKancharla')
--	Begin 
--		ALTER DATABASE [DWClinicReportDataKrishnaKancharla] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
--		DROP DATABASE [DWClinicReportDataKrishnaKancharla]
--	End
--GO
--Create Database [DWClinicReportDataKrishnaKancharla]
--Go


Use DWClinicReportDataKrishnaKancharla;
go
SET NoCount ON;


--********************************************************************--
-- Add Logging Support: Create Table, View and Stored Procedure
--********************************************************************--
If NOT Exists(Select * From Sys.tables where Name = 'ETLLog')
  Create -- Drop
  Table ETLLog
  (ETLLogID int identity Primary Key
  ,ETLDateAndTime datetime Default GetDate()
  ,ETLAction varchar(100)
  ,ETLLogMessage varchar(2000)
  );
go

Create or Alter View vETLLog
As
  Select
   ETLLogID
  ,ETLDate = Format(ETLDateAndTime, 'D', 'en-us')
  ,ETLTime = Format(Cast(ETLDateAndTime as datetime2), 'HH:mm', 'en-us')
  ,ETLAction
  ,ETLLogMessage
  From ETLLog;
go
--select * from vETLLog;

Create or Alter Procedure pInsETLLog
 (@ETLAction varchar(100), @ETLLogMessage varchar(2000))
--*************************************************************************--
-- Desc:This Sproc creates an admin table for logging ETL metadata. 
-- Change Log: When,Who,What
-- 2022-02-27,KKancharla,Created Sproc
--*************************************************************************--
As
Begin
  Declare @RC int = 0;
  Begin Try
    Begin Tran;
      Insert Into ETLLog
       (ETLAction,ETLLogMessage)
      Values
       (@ETLAction,@ETLLogMessage)
    Commit Tran;
    Set @RC = 1;
  End Try
  Begin Catch
    If @@TRANCOUNT > 0 Rollback Tran;
    Set @RC = -1;
  End Catch
  Return @RC;
End
go 
/* Testing Code:
 Declare @Status int;
 Exec @Status = pInsETLLog 'Test', 'Test message';
 Print @Status;
 Select * From vETLLog;
*/

--********************************************************************--
-- Pre-Load Tasks: Procedure to clear tables
--********************************************************************--
go
Create or Alter Procedure pETLTruncateTables
/* Author: KKancharla
** Desc: Flushes all date from the tables
** Change Log: When,Who,What
** 2022-02-27,KKancharla,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try

    -- ETL Processing Code --
	Truncate Table [DWClinicReportDataKrishnaKancharla].dbo.FactDoctorShifts;
	Truncate Table [DWClinicReportDataKrishnaKancharla].dbo.FactVisits;
	Truncate Table [DWClinicReportDataKrishnaKancharla].dbo.DimClinics;
	Truncate Table [DWClinicReportDataKrishnaKancharla].dbo.DimDoctors;
	Truncate Table [DWClinicReportDataKrishnaKancharla].dbo.DimShifts; 
	Truncate Table [DWClinicReportDataKrishnaKancharla].dbo.DimProcedures;
	Truncate Table [DWClinicReportDataKrishnaKancharla].dbo.DimPatients;
    Exec pInsETLLog
	        @ETLAction = 'pETLTruncateTables'
	       ,@ETLLogMessage = 'Tables data removed';
    Set @RC = +1
  End Try
  Begin Catch
     Declare @ErrorMessage nvarchar(1000) = Error_Message();
	 Exec pInsETLLog 
	      @ETLAction = 'pETLTruncateTables'
	     ,@ETLLogMessage = @ErrorMessage;
    Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLTruncateTables;
 Print @Status;
 Select * From vETLLog;
*/

--select * from DimDates;
--select * from DimClinics;
--select * from DimDoctors;
--select * from DimPatients;
--select * from DimProcedures;
--select * from DimShifts;

--********************************************************************--
-- B) FILL the Tables
--********************************************************************--

/****** [dbo].[DimDates] ******/
Create or Alter Procedure pETLFillDimDates
/* Author: KKancharla
** Desc: Inserts data Into DimDates
** Change Log: When,Who,What
** 20220227, KKancharla,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try

    -- ETL Processing Code --
      Declare @StartDate datetime = '01/01/2004'
      Declare @EndDate datetime = '12/31/2020' 
      Declare @DateInProcess datetime  = @StartDate
      -- Loop through the dates until you reach the end date
      While @DateInProcess <= @EndDate
       Begin
       -- Add a row Into the date dimension table for this date
       Insert Into dbo.DimDates 
       ( --[DateKey]
	   [FullDate]
	   ,[FullDateName]
	   ,[MonthID]
	   ,[MonthName]
	   ,[YearID]
	   ,[YearName])
       Values ( 
        -- Cast(Convert(nVarchar(50), @DateInProcess, 112) as int) -- [DateKey]
        @DateInProcess -- [FullDate]
        ,DateName(weekday, @DateInProcess) + ', ' + Convert(nVarchar(50), @DateInProcess, 110) -- [DateName]  
        ,Cast(Left(Convert(nVarchar(50), @DateInProcess, 112), 6) as int)  -- [MonthID]
        ,DateName(month, @DateInProcess) + ' - ' + DateName(YYYY,@DateInProcess) -- [MonthName]
        ,Year(@DateInProcess) -- [YearID] 
        ,Cast(Year(@DateInProcess ) as nVarchar(50)) -- [YearName] 
        )  
       -- Add a day and loop again
       Set @DateInProcess = DateAdd(d, 1, @DateInProcess)
       End

    Exec pInsETLLog
	        @ETLAction = 'pETLFillDimDates'
	       ,@ETLLogMessage = 'DimDates filled';
    Set @RC = +1
  End Try
  Begin Catch
     Declare @ErrorMessage nvarchar(1000) = Error_Message();
	 Exec pInsETLLog 
	      @ETLAction = 'pETLFillDimDates'
	     ,@ETLLogMessage = @ErrorMessage;
    Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLFillDimDates;
 Print @Status;
 Select * From DimDates;
 Select * From vETLLog;
*/


DELETE FROM DimClinics
DBCC CHECKIDENT ('DimClinics', RESEED, 0)

/********** [DimClinics] ***********/
go 
Create or Alter View vETLDimClinics
/* Author: KKancharla
** Desc: Extracts and transforms data for DimClinics
** Change Log: When,Who,What
** 2022-02-27, KKancharla,Created view.
*/
As
  Select
		[ClinicID] = c.[ClinicID] 
	   ,[ClinicName] = Cast(c.ClinicName as nvarchar(100))
	   ,[ClinicCity] = Cast(c.City as nvarchar(100))
	   ,[ClinicState] = Cast(c.State as nvarchar(100))
	   ,[ClinicZip] = Cast(c.Zip as nVarchar(5))
  From [DoctorsSchedules].dbo.Clinics as c
go
/* Testing Code:
 Select * From vETLDimClinics;
*/

go
Create or Alter Procedure pETLFillDimClinics
/* Author: KKancharla
** Desc: Inserts data Into DimClinics using the vETLDimClinics view
** Change Log: When,Who,What
** 20220227,KKancharla,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try

    -- ETL Processing Code --
  	Set NoCount On;
    --1)  new INSERT. This code inserts only new  rows!
		With NewOrChangedClinics
		As( Select [ClinicID], [ClinicName], [ClinicCity], [ClinicState], [ClinicZip] From vETLDimClinics
		Except
		Select [ClinicID], [ClinicName], [ClinicCity], [ClinicState], [ClinicZip] From [DWClinicReportDataKrishnaKancharla].[dbo].[DimClinics]
		) Insert Into [DWClinicReportDataKrishnaKancharla].[dbo].[DimClinics]
		([ClinicID], [ClinicName], [ClinicCity], [ClinicState], [ClinicZip])
		Select [ClinicID], [ClinicName], [ClinicCity], [ClinicState], [ClinicZip]
		From vETLDimClinics
		Where [ClinicID] IN (Select [ClinicID] From NewOrChangedClinics) and ClinicID not in (SELECT ClinicID FROM [DWClinicReportDataKrishnaKancharla].[dbo].[DimClinics]);

	-- 2) update the row
		With UpdatedClinics
		As( Select [ClinicID], [ClinicName], [ClinicCity], [ClinicState], [ClinicZip] From vETLDimClinics
		Except
		 Select [ClinicID], [ClinicName], [ClinicCity], [ClinicState], [ClinicZip] From [DWClinicReportDataKrishnaKancharla].[dbo].[DimClinics]
		) UPDATE [DimClinics] set [ClinicName] = (SELECT [ClinicName] from UpdatedClinics WHERE UpdatedClinics.[ClinicID] = [DWClinicReportDataKrishnaKancharla].[dbo].[DimClinics].ClinicID),
								   [ClinicCity] = (SELECT [ClinicCity] from UpdatedClinics WHERE UpdatedClinics.[ClinicID] = [DWClinicReportDataKrishnaKancharla].[dbo].[DimClinics].ClinicID),
								   [ClinicState] = (SELECT [ClinicState] from UpdatedClinics WHERE UpdatedClinics.[ClinicID] = [DWClinicReportDataKrishnaKancharla].[dbo].[DimClinics].ClinicID),
								   [ClinicZip] = (SELECT [ClinicZip] from UpdatedClinics WHERE UpdatedClinics.[ClinicID] = [DWClinicReportDataKrishnaKancharla].[dbo].[DimClinics].ClinicID)
		WHERE [ClinicID] in (SELECT [ClinicID] from UpdatedClinics)
		;
	-- 3) delete the row
		With deletedClinics
		As( Select [ClinicID], [ClinicName], [ClinicCity], [ClinicState], [ClinicZip] From [DWClinicReportDataKrishnaKancharla].[dbo].[DimClinics]
		Except
		Select [ClinicID], [ClinicName], [ClinicCity], [ClinicState], [ClinicZip] From vETLDimClinics
		)Delete From [DWClinicReportDataKrishnaKancharla].[dbo].[DimClinics] WHERE [ClinicID]  in (SELECT [ClinicID] FROM deletedClinics);

    Exec pInsETLLog
	        @ETLAction = 'pETLFillDimClinics'
	       ,@ETLLogMessage = 'DimClinics filled';
    Set @RC = +1
  End Try
  Begin Catch
     Declare @ErrorMessage nvarchar(1000) = Error_Message();
	 Exec pInsETLLog 
	      @ETLAction = 'pETLFillDimClinics'
	     ,@ETLLogMessage = @ErrorMessage;
    Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLFillDimClinics;
 Print @Status;
 Select * From DimClinics;
 Select * From vETLLog;
*/


/********** [DimDoctors] ***********/
go 
Create or Alter View vETLDimDoctors
/* Author: KKancharla
** Desc: Extracts and transforms data for DimDoctors
** Change Log: When,Who,What
** 2022-02-27, KKancharla,Created view.
*/
As
  Select
		[DoctorID] = d.DoctorID
	   ,[DoctorFullName] = Concat(d.FirstName, ' ', d.LastName)
	   ,[DoctorEmailAddress] = Cast(d.EmailAddress as nvarchar(100))
	   ,[DoctorCity] = Cast(d.City as nvarchar(100))
	   ,[DoctorState] = Cast(d.State as nvarchar(100))
	   ,[DoctorZip] = d.Zip
  From [DoctorsSchedules].dbo.Doctors as d
go
/* Testing Code:
 Select * From vETLDimDoctors;
*/

go
Create or Alter Procedure pETLFillDimDoctors
/* Author: KKancharla
** Desc: Inserts data Into DimDoctors using the vETLDimDoctors view
** Change Log: When,Who,What
** 20220227,KKancharla,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try

    -- ETL Processing Code --
    If ((Select Count(*) From DimDoctors) = 0)
     Begin
      Insert Into [DWClinicReportDataKrishnaKancharla].dbo.DimDoctors	
	    ([DoctorID] 
	   ,[DoctorFullName] 
	   ,[DoctorEmailAddress] 
	   ,[DoctorCity] 
	   ,[DoctorState] 
	   ,[DoctorZip] 
      )
      Select
        [DoctorID] 
	   ,[DoctorFullName] 
	   ,[DoctorEmailAddress] 
	   ,[DoctorCity] 
	   ,[DoctorState] 
	   ,[DoctorZip] 
      FROM vETLDimDoctors
    End

    Exec pInsETLLog
	        @ETLAction = 'pETLFillDimDoctors'
	       ,@ETLLogMessage = 'DimDoctors filled';
    Set @RC = +1
  End Try
  Begin Catch
     Declare @ErrorMessage nvarchar(1000) = Error_Message();
	 Exec pInsETLLog 
	      @ETLAction = 'pETLFillDimDoctors'
	     ,@ETLLogMessage = @ErrorMessage;
    Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLFillDimDoctors;
 Print @Status;
 Select * From DimDoctors;
 Select * From vETLLog;
*/


/********** [DimShifts] ***********/
go 
Create or Alter View vETLDimShifts
/* Author: KKancharla
** Desc: Extracts and transforms data for DimShifts
** Change Log: When,Who,What
** 2022-02-27, KKancharla,Created view.
*/
As
  Select
		[ShiftID] = s.ShiftID
	   ,[ShiftStart] = iif(s.ShiftStart = '01:00', '13:00', s.ShiftStart)
	   ,[ShiftEnd] = iif(s.ShiftEnd = '05:00', '17:00', s.ShiftEnd)
  From [DoctorsSchedules].dbo.Shifts as s
go
/* Testing Code:
 Select * From vETLDimShifts;
*/

go
Create or Alter Procedure pETLFillDimShifts
/* Author: KKancharla
** Desc: Inserts data Into DimDoctors using the vETLDimShifts view
** Change Log: When,Who,What
** 20220227,KKancharla,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try

    -- ETL Processing Code --
    If ((Select Count(*) From DimShifts) = 0)
     Begin
      Insert Into [DWClinicReportDataKrishnaKancharla].dbo.DimShifts	
	    ([ShiftID]
		,[ShiftStart]
		,[ShiftEnd]
      )
      Select
		 [ShiftID]
		,[ShiftStart]
		,[ShiftEnd]
      FROM vETLDimShifts
    End

    Exec pInsETLLog
	        @ETLAction = 'pETLFillDimShifts'
	       ,@ETLLogMessage = 'DimShifts filled';
    Set @RC = +1
  End Try
  Begin Catch
     Declare @ErrorMessage nvarchar(1000) = Error_Message();
	 Exec pInsETLLog 
	      @ETLAction = 'pETLFillDimShifts'
	     ,@ETLLogMessage = @ErrorMessage;
    Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLFillDimShifts;
 Print @Status;
 Select * From DimShifts;
 Select * From vETLLog;
*/


/********** [DimPatients] ***********/
go 
Create or Alter View vETLDimPatients
/* Author: KKancharla
** Desc: Extracts and transforms data for DimPatients
** Change Log: When,Who,What
** 2022-02-27, KKancharla,Created view.
*/
As
  Select 
		 [PatientID] = p.ID
		,[PatientFullName] = Concat(p.FName, ' ', p.LName)
		,[PatientCity] = p.City
		,[PatientState] = p.State
		,[PatientZipCode] = p.ZipCode
		,[StartDate] = GETDATE()
		,[EndDate] = NULL
		,[IsCurrent] = 1
  From [Patients].dbo.Patients p
go
/* Testing Code:
 Select * From vETLDimPatients;
*/

go
Create or Alter Procedure pETLFillDimPatients
/* Author: KKancharla
** Desc: Inserts data Into DimPatients using the vETLDimPatients view
** Change Log: When,Who,What
** 20220227,KKancharla,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try

    -- ETL Processing Code --
    If ((Select Count(*) From DimPatients) = 0)
     Begin
      Insert Into [DWClinicReportDataKrishnaKancharla].dbo.DimPatients	
	    ([PatientID] 
		,[PatientFullName] 
		,[PatientCity] 
		,[PatientState] 
		,[PatientZipCode] 
		,[StartDate]
		,[EndDate] 
		,[IsCurrent]  
      )
      Select
		 [PatientID] 
		,[PatientFullName] 
		,[PatientCity] 
		,[PatientState] 
		,[PatientZipCode] 
		,[StartDate] = GETDATE()
		,[EndDate] = NULL
		,[IsCurrent] = 1
      FROM vETLDimPatients
    End

    Exec pInsETLLog
	        @ETLAction = 'pETLFillDimPatients'
	       ,@ETLLogMessage = 'DimPatients filled';
    Set @RC = +1
  End Try
  Begin Catch
     Declare @ErrorMessage nvarchar(1000) = Error_Message();
	 Exec pInsETLLog 
	      @ETLAction = 'pETLFillDimPatients'
	     ,@ETLLogMessage = @ErrorMessage;
    Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLFillDimPatients;
 Print @Status;
 Select * From DimPatients;
 Select * From vETLLog;
*/



/********** [DimProcedures] ***********/
go 
Create or Alter View vETLDimProcedures
/* Author: KKancharla
** Desc: Extracts and transforms data for DimProcedures
** Change Log: When,Who,What
** 2022-02-27, KKancharla,Created view.
*/
As
  Select
		[ProcedureID] = pc.ID
		,[ProcedureName] = pc.[Name]
		,[ProcedureDesc] = pc.[Desc]
		,[ProcedureCharge] = pc.Charge
  From [Patients].dbo.[Procedures] as pc
go
/* Testing Code:
 Select * From vETLDimProcedures;
*/

go
Create or Alter Procedure pETLFillDimProcedures
/* Author: KKancharla
** Desc: Inserts data Into DimDoctors using the vETLDimProcedures view
** Change Log: When,Who,What
** 20220227,KKancharla,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try

    -- ETL Processing Code --
    If ((Select Count(*) From DimProcedures) = 0)
     Begin
      Insert Into [DWClinicReportDataKrishnaKancharla].dbo.DimProcedures	
	    ([ProcedureID] 
		,[ProcedureName] 
		,[ProcedureDesc] 
		,[ProcedureCharge] 
      )
      Select
		 [ProcedureID] 
		,[ProcedureName] 
		,[ProcedureDesc] 
		,[ProcedureCharge] 
      FROM vETLDimProcedures
    End

    Exec pInsETLLog
	        @ETLAction = 'pETLFillDimProcedures'
	       ,@ETLLogMessage = 'DimProcedures filled';
    Set @RC = +1
  End Try
  Begin Catch
     Declare @ErrorMessage nvarchar(1000) = Error_Message();
	 Exec pInsETLLog 
	      @ETLAction = 'pETLFillDimProcedures'
	     ,@ETLLogMessage = @ErrorMessage;
    Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLFillDimProcedures;
 Print @Status;
 Select * From DimProcedures;
 Select * From vETLLog;
*/


/********** [FactDoctorShifts] ***********/
go 
Create or Alter View vETLFactDoctorShifts
/* Author: KKancharla
** Desc: Extracts and transforms data for FactDoctorShifts
** Change Log: When,Who,What
** 2022-02-27, KKancharla,Created view.
*/
As
  Select
		[DoctorsShiftID] = doc.DoctorsShiftID
		,[ShiftDateKey] = dd.DateKey
		,[ClinicKey] = dc.ClinicKey
		,[ShiftKey] = ds.ShiftKey
		,[DoctorKey] = d.DoctorKey
		,[HoursWorked] = abs(Datediff(hh, ds.ShiftStart, ds.ShiftEnd))
  From [DoctorsSchedules].dbo.DoctorShifts as doc
  Join [DWClinicReportDataKrishnaKancharla].dbo.DimDates as dd on doc.ShiftDate = dd.FullDate
  Join [DWClinicReportDataKrishnaKancharla].dbo.DimClinics as dc on doc.ClinicID = dc.ClinicID
  Join [DWClinicReportDataKrishnaKancharla].dbo.DimShifts as ds on doc.ShiftID = ds.ShiftID
  Join [DWClinicReportDataKrishnaKancharla].dbo.DimDoctors as d on doc.DoctorID = d.DoctorID
go
/* Testing Code:
 Select * From vETLFactDoctorShifts;
*/
select * from DimClinics;

go
Create or Alter Procedure pETLFillFactDoctorShifts
/* Author: KKancharla
** Desc: Inserts data Into FactDoctorShifts using the vETLFactDoctorShifts view
** Change Log: When,Who,What
** 20220227,KKancharla,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try

    -- ETL Processing Code --
		Set NoCount On;	
		Merge Into FactDoctorShifts as t
			Using vETLFactDoctorShifts as s 
			On t.DoctorsShiftID = s.DoctorsShiftID
			And t.ShiftDateKey = s.ShiftDateKey
			And t.ClinicKey = s.ClinicKey
			And t.ShiftKey = s.ShiftKey
			And t.DoctorKey = s.DoctorKey
			And t.HoursWorked = s.HoursWorked			 
			When Not Matched By Target 
			 Then
				Insert (DoctorsShiftID, ShiftDateKey, ClinicKey, ShiftKey, DoctorKey, HoursWorked)
				Values (DoctorsShiftID, ShiftDateKey, ClinicKey, ShiftKey, DoctorKey, HoursWorked)
		    When Not Matched By Source
			Then
			    Delete;
		Set NoCount Off;

    Exec pInsETLLog
	        @ETLAction = 'pETLFillFactDoctorShifts'
	       ,@ETLLogMessage = 'FactDoctorShifts filled';
    Set @RC = +1
  End Try
  Begin Catch
     Declare @ErrorMessage nvarchar(1000) = Error_Message();
	 Exec pInsETLLog 
	      @ETLAction = 'pETLFillFactDoctorShifts'
	     ,@ETLLogMessage = @ErrorMessage;
    Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLFillFactDoctorShifts;
 Print @Status;
 Select * From FactDoctorShifts;
 Select * From vETLLog;
*/



/********** [FactVisits] ***********/
go 
Create or Alter View vETLFactVisits
/* Author: KKancharla
** Desc: Extracts and transforms data for FactVisits
** Change Log: When,Who,What
** 2022-02-27, KKancharla,Created view.
*/
As
  Select
		[VisitID]  = v.ID
		,[DateKey] = dd.DateKey 
		,[ClinicKey] = dc.ClinicKey
		,[PatientKey] = dp.PatientKey
		,[DoctorKey] = d.DoctorKey
		,[ProcedureKey] = dproc.ProcedureKey
		,[ProcedureVistCharge] = dpro.ProcedureCharge
  From [Patients].dbo.Visits as v
    Join [DWClinicReportDataKrishnaKancharla].dbo.DimDates as dd on dd.FullDate = convert(varchar(10),v.[date],111)
    Join [DWClinicReportDataKrishnaKancharla].dbo.DimClinics as dc on v.Clinic = dc.ClinicID * 100
	Join [DWClinicReportDataKrishnaKancharla].dbo.DimPatients as dp on v.Patient = dp.PatientID
    Join [DWClinicReportDataKrishnaKancharla].dbo.DimProcedures as dproc on v.[Procedure] = dproc.ProcedureID
	Join [DWClinicReportDataKrishnaKancharla].dbo.DimProcedures as dpro on v.Charge = dpro.ProcedureCharge
	Join [DWClinicReportDataKrishnaKancharla].dbo.DimDoctors as d on v.Doctor = d.DoctorID
go
/* Testing Code:
 Select * From vETLFactVisits;
*/

--select visitid, datekey, clinickey, count(*) as dups from vETLFactVisits group by visitid, datekey, clinickey order by dups;

go
Create or Alter Procedure pETLFillFactVisits
/* Author: KKancharla
** Desc: Inserts data Into FactVisits using the vETLFactVisits view
** Change Log: When,Who,What
** 20220227,KKancharla,Created Sproc.
*/
AS
 Begin
  Declare @RC int = 0;
  Begin Try

    -- ETL Processing Code --
    If ((Select Count(*) From FactVisits) = 0)
     Begin
      Insert Into [DWClinicReportDataKrishnaKancharla].dbo.FactVisits	
	    ([VisitID]  
		,[DateKey] 
		,[ClinicKey] 
		,[PatientKey] 
		,[DoctorKey] 
		,[ProcedureKey] 
		,[ProcedureVistCharge]
      )
      Select
			 [VisitID]  
			,[DateKey] 
			,[ClinicKey] 
			,[PatientKey] 
			,[DoctorKey] 
			,[ProcedureKey] 
			,[ProcedureVistCharge]
      FROM vETLFactVisits 
    End

    Exec pInsETLLog
	        @ETLAction = 'pETLFillFactVisits'
	       ,@ETLLogMessage = 'FactVisits filled';
    Set @RC = +1
  End Try
  Begin Catch
     Declare @ErrorMessage nvarchar(1000) = Error_Message();
	 Exec pInsETLLog 
	      @ETLAction = 'pETLFillFactVisits'
	     ,@ETLLogMessage = @ErrorMessage;
    Set @RC = -1
  End Catch
  Return @RC;
 End
go
/* Testing Code:
 Declare @Status int;
 Exec @Status = pETLFillFactVisits;
 Print @Status;
 Select * From FactVisits;
 Select * From vETLLog;
*/


--select * from vETLDimClinics;
--select * from vETLDimDoctors;
--select * from vETLDimPatients;
--select * from vETLDimProcedures;
--select * from vETLDimShifts;
--Select * From FactVisits;

--select * from DimDates;
--select * from DimClinics;
--select * from DimDoctors;
--select * from DimPatients;
--select * from DimProcedures;
--select * from DimShifts;

--Select * 
--from dbo.FactVisits as fv
--join dbo.DimClinics as dc
--on fv.ClinicKey = dc.ClinicKey
--join dbo.DimDates as dd
--on fv.DateKey = dd.DateKey
--join dbo.DimDoctors as ddoc
--on fv.DoctorKey = ddoc.DoctorKey
--join dbo.DimPatients as dp
--on fv.PatientKey = dp.PatientKey
--join dbo.DimProcedures as dpr
--on fv.ProcedureKey = dpr.ProcedureKey;