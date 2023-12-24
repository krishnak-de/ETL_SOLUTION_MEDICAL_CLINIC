/***************************************************************************
DWClinicReportData_KrishnaKancharla
Desc: This is a Data Warehouse for the Patient and DoctorsSchedule Databases.
	  ETL processing issues.
Who: Krishna Kancharla
When: 02/21/2021
ChangeLog: 
	Removed addresses from DimPatients
	Removed addresses from DimDoctors and DimClinic
	Altered the file description
	Added names to all PK and FK constraints
	Added SCD columns to DimPatients
	Added ETL logging tables
*****************************************************************************************/
Use Master;
go

If Exists (Select * From Sys.databases where Name = 'DWClinicReportDataKrishnaKancharla')
  Begin
   Alter Database DWClinicReportDataKrishnaKancharla set single_user with rollback immediate;
   Drop Database DWClinicReportDataKrishnaKancharla;
  End
go

Create Database DWClinicReportDataKrishnaKancharla;
go

Use DWClinicReportDataKrishnaKancharla;
go


Create Table DimDates -- Type 1 SCD
(DateKey int Constraint pkDimDates Primary Key Identity 
,FullDate datetime Not Null
,FullDateName nvarchar (50) Not Null 
,MonthID int Not Null
,[MonthName] nvarchar(50) Not Null
,YearID int Not Null
,YearName nvarchar(50) Not Null
);
go

Create Table DimClinics -- Type 1 SCD
(ClinicKey int Constraint pkDimClinics Primary Key Identity
,ClinicID int Not Null
,ClinicName nvarchar(100) Not Null 
,ClinicCity nvarchar(100) Not Null
,ClinicState nvarchar(100) Not Null 
,ClinicZip nvarchar(5) Not Null 
);
go

Create Table DimDoctors -- Type 1 SCD
(DoctorKey int Constraint pkDimDoctors Primary Key Identity
,DoctorID int Not Null  
,DoctorFullName nvarchar(200) Not Null 
,DoctorEmailAddress nvarchar(100) Not Null  
,DoctorCity nvarchar(100) Not Null
,DoctorState nvarchar(100) Not Null
,DoctorZip nvarchar(5) Not Null 
);
go

Create Table DimShifts -- Type 1 SCD
(ShiftKey int Constraint pkDimShifts Primary Key Identity
,ShiftID int Not Null
,ShiftStart time(0) Not Null
,ShiftEnd time(0) Not Null
);
go

Create Table FactDoctorShifts -- Type 1 SCD
(DoctorsShiftID int Not Null
,ShiftDateKey int Constraint fkFactDoctorShiftsToDimDates References DimDates(DateKey) Not Null
,ClinicKey int Constraint fkFactDoctorShiftsToDimClinics References DimClinics(ClinicKey) Not Null
,ShiftKey int Constraint fkFactDoctorShiftsToDimShifts References DimShifts(ShiftKey) Not Null
,DoctorKey int Constraint fkFactDoctorShiftsToDimDoctors References DimDoctors(DoctorKey) Not Null
,HoursWorked int
Constraint pkFactDoctorShifts Primary Key(DoctorsShiftID, ShiftDateKey , ClinicKey, ShiftKey, DoctorKey)
);
go

Create Table DimProcedures -- Type 1 SCD
(ProcedureKey int Constraint pkDimProcedures Primary Key Identity
,ProcedureID int Not Null
,ProcedureName varchar(100) Not Null
,ProcedureDesc varchar(1000) Not Null
,ProcedureCharge money Not Null 
);
go

Create Table DimPatients -- Type 2 SCD
(PatientKey int Constraint pkDimPatients Primary Key Identity
,PatientID int Not Null
,PatientFullName varchar(100) Not Null
,PatientCity varchar(100) Not Null
,PatientState varchar(100) Not Null
,PatientZipCode int Not Null
,StartDate date Not Null
,EndDate date Null
,IsCurrent int Constraint ckDimPatientsIsCurrent Check (IsCurrent In (1,0))
);
go

Create Table FactVisits -- Type 1 SCD
(VisitID int Not Null 
,DateKey int Constraint fkFactVisitsToDimDates References DimDates(DateKey) Not Null
,ClinicKey int Constraint fkFactVisitsToDimClinics References DimClinics(ClinicKey) Not Null
,PatientKey int Constraint fkFactVisitsToDimPatients References DimPatients(PatientKey) Not Null
,DoctorKey int Constraint fkFactVisitsToDimDoctors References DimDoctors(DoctorKey) Not Null
,ProcedureKey int Constraint fkFactVisitsToDimProcedures References DimProcedures(ProcedureKey) Not Null 
,ProcedureVistCharge money Not Null
Constraint pkFactVisits Primary Key(VisitID, DateKey, ClinicKey, PatientKey, DoctorKey, ProcedureKey)
);
go

--********************************************************************--
--  Create ETL logging objects. Use these in your ETL stored procedures!
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


Create or Alter Proc pInsETLLog
 (@ETLAction varchar(100), @ETLLogMessage varchar(2000))
--*************************************************************************--
-- Desc:This Sproc creates an admin table for logging ETL metadata. 
-- Change Log: When,Who,What
-- 2020-01-01,RRoot,Created Sproc
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
Go

--select * from DimClinics;
--select * from DimDates;
--select * from DimDoctors;
--select * from DimPatients;
--select * from DimProcedures;
--select * from DimShifts;