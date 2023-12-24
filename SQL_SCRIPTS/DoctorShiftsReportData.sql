/************************************************************************************************
Desc: This file creates report views that provide data for MongoDB.
Dev: KKancharla
Date: 2022-03-11
Change Log: (When, Who, What)
22022-03-11, KKancharla, Created two views for doctorshifts and patients visits reporting
************************************************************************************************/
Use [DWClinicReportDataKrishnaKancharla];
go


If (Object_ID('vRptDoctorShifts') is not null) Drop View vRptDoctorShifts;
go
Create View vRptDoctorShifts
As
Select Top 1000
 [ShiftDate] = Cast(Cast([FullDate] as date) as varchar(100))
,[ClinicName] = dc.[ClinicName]
,[ClinicCity] = dc.[ClinicCity]
,[ClinicState] = dc.[ClinicState]
,[ShiftID] = ds.[ShiftID]
,[ShiftStart] = ds.[ShiftStart]
,[ShiftEnd] = ds.[ShiftEnd]
,[DoctorFullName] = ddc.[DoctorFullName]
,[HoursWorked] = fds.[HoursWorked]
From [dbo].[FactDoctorShifts] as fds
Join [dbo].[DimDates] as dd
 On fds.[ShiftDateKey] = dd.[DateKey]
Join [dbo].[DimClinics] as dc
 On fds.[ClinicKey] = dc.[ClinicKey]
Join [dbo].[DimShifts] as ds
 On fds.[ShiftKey] = ds.[ShiftKey]
Join [dbo].[DimDoctors] as ddc
 On fds.[DoctorKey] = ddc.[DoctorKey]
Order By [ShiftDate];
go

--select * from vRptDoctorShifts;


If (Object_ID('vRptPatientVisits') is not null) Drop View vRptPatientVisits;
go
Create View vRptPatientVisits
As
Select Top 1000
 [VisitDate] = Cast(Cast([FullDate] as date) as varchar(100))
,[ClinicName] = dc.[ClinicName]
,[ClinicCity] = dc.[ClinicCity]
,[ClinicState] = dc.[ClinicState]
,[PatientFullName] = dp.[PatientFullName]
,[PatientCity] = dp.[PatientCity]
,[PatientState] = dp.[PatientState]
,[DoctorFullName] = ddc.[DoctorFullName]
,[ProcedureName] = dpr.[ProcedureName]  
,[ProcedureVisitCharge] = fv.[ProcedureVistCharge]
From [dbo].[FactVisits] as fv
Join [dbo].[DimDates] as dd On fv.[DateKey] = dd.[DateKey]
Join [dbo].[DimClinics] dc On fv.[ClinicKey] = dc.[ClinicKey]
Join [dbo].[DimPatients] as dp On fv.[PatientKey] = dp.[PatientKey]
Join [dbo].[DimDoctors] ddc On fv.[DoctorKey] = ddc.[DoctorKey]
Join [dbo].[DimProcedures] as dpr On fv.[ProcedureKey] = dpr.[ProcedureKey]
Where dp.[IsCurrent] = 1
Order By [VisitDate];
go

--Select * From vRptPatientVisits;