# %% Import statements
import os
import sys
import yaml
import pandas as pd
import numpy as np 
import pathlib
import datetime
import regex
import sqlalchemy
import urllib
import collections.abc

# %% Define global variables
default_database = "CCDW"
default_server = "localhost"

current_directory = os.getcwd()

# %% Define global functions
run_datetime = datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S%f")
def timestamp():
    return( datetime.datetime.now().isoformat() )

#import config
def load_cfg():
    with open("config.yml","r") as ymlfile:
        cfg_l = yaml.load(ymlfile, Loader=yaml.FullLoader)

        if cfg_l["config"]["location"] == "self":
            cfg = cfg_l.copy()
        else:
            with open(cfg_l["config"]["location"] + "config.yml","r") as ymlfile2:
                cfg = yaml.load(ymlfile2, Loader=yaml.FullLoader)
    return cfg

def getColleagueData( engine, file, cols=[], schema="history", version="latest", where="", sep='.' ):

    if isinstance(cols,collections.abc.Mapping):
        qry_cols = '*' if cols == [] else ', '.join([f"[{c}] AS [{cols[c]}]" for c in cols])
    else:
        qry_cols = '*' if cols == [] else ', '.join([f"[{c}]" for c in cols])
        
    qry_where = "" if where == "" else f"WHERE {where}"
    
    if (version == "latest" and schema=="history"):
        qry_where = "WHERE " if where == "" else qry_where + " AND "
        qry_where += f"CurrentFlag='Y'"
                
    qry = f"SELECT {qry_cols} FROM {schema}.{file} {qry_where}"
    
    df = pd.read_sql(qry,engine)

    if (sep != '.'):
        df.columns = df.columns.str.replace(".", sep)

    return(df)

# %% Load configuration and define SQL engine
cfg = load_cfg() 

conn_details = urllib.parse.quote_plus(
    f"DRIVER={{{cfg['sql']['driver']}}};"
    f"SERVER={cfg['sql']['server']};"
    f"DATABASE={cfg['sql']['db']};"
    f"SCHEMA={cfg['sql']['schema']};"
    f"Trusted_Connection=Yes;"
    f"Description=Python.HEERF_Report")

engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % conn_details)

# %% Load PERSON data
person_cols = { "ID"         : "Student_ID",
                "FIRST.NAME" : "First_Name",
                "LAST.NAME"  : "Last_Name"
              }
person_where = "COALESCE([FIRST.NAME],'') <> ''"
person = getColleagueData( engine, "PERSON", 
                           cols = person_cols, 
                           where = person_where
                         )

# %% Load FAFSA data
fafsa_cols = { "ISIR.FAFSA.ID"    : "FAFSA_ID",
               "IFAF.STUDENT.ID"  : "Student_ID",
               "IFAF.PRI.EFC"     : "EFC"
             }
fafsa_where = f"[IFAF.IMPORT.YEAR] = '{award_year_last}' AND COALESCE([IFAF.PRI.EFC],-1) >= 0 AND COALESCE([IFAF.TRANS.RECEIPT.DT],'') <> ''"
fafsa = getColleagueData( engine, "ISIR_FAFSA", 
                          cols = fafsa_cols, 
                          where = fafsa_where
                        )

# %% Load CS.ACYR data
csacyr_cols = { "CS.STUDENT.ID"             : "Student_ID",
                "CS.VERIF.DT"               : "Verification_Date",
                "CS.VERIF.STATUS"           : "Verification_Status",
                "CS.FED.ISIR.OR.CORRECTION" : "FAFSA_ID"
              }
csacyr_where = f"[CS.YEAR] IN ('{award_year_last}') AND COALESCE([CS.FED.ISIR.OR.CORRECTION],'')<>''"
csacyr = getColleagueData( engine, "CS_ACYR", 
                           cols = csacyr_cols, 
                           where = csacyr_where
                         )
csacyr["Student_ID"] = csacyr['Student_ID'].apply(lambda r: f"0000000{r}"[-7:])
csacyr_fafsa = pd.merge(csacyr, fafsa, on=["Student_ID","FAFSA_ID"], how='inner')
csacyr_fafsa.drop("FAFSA_ID",axis=1,inplace=True)

# %% Load COURSE_SECTIONS data
courses_cols = { "COURSE.SECTIONS.ID"      : "Course_Sections_ID",
                 "SEC.TERM"                : "Term",
                 "SEC.COURSE"              : "Course_ID",
                 "X.SEC.DELIVERY.METHOD"   : "Delivery_Method"
               }
courses_where = f"[SEC.ACAD.LEVEL] = 'CU' AND [SEC.TERM] = '{term}'"
courses = getColleagueData( engine, "COURSE_SECTIONS", 
                            cols = courses_cols, 
                            where = courses_where
                          )

# %% Load STUDENT_ACAD_CRED for a particular date
student_courses_date_cols = { "STC.PERSON.ID"      : "Student_ID",
                              "STC.TERM"           : "Term",
                              "STC.COURSE"         : "Course_ID",
                              "STC.COURSE.SECTION" : "Course_Sections_ID",
                              "STC.CRED"           : "Credits_Attempted",
                              "STC.STATUS"         : "Course_Status",
                              "EffectiveDatetime"  : "EffectiveDatetime"
                             }
student_courses_date_where = f"[STC.ACAD.LEVEL] = 'CU' AND [STC.TERM] = '{term}' AND [EffectiveDatetime] <= '{enrollment_date}'"
student_courses_date = getColleagueData( engine, "STUDENT_ACAD_CRED", 
                                    cols = student_courses_date_cols, 
                                    where = student_courses_date_where,
                                    version = "history"
                                   )

# %% Filter STUDENT_ACAD_CRED date to the most recent record and keep only active courses
student_courses_date = student_courses_date.sort_values(["Student_ID","Course_Sections_ID","EffectiveDatetime"], ascending=False).drop_duplicates(["Student_ID","Course_Sections_ID"])
student_courses_date = student_courses_date.loc[student_courses_date["Course_Status"].isin(['A','N']),["Student_ID","Term","Course_ID","Course_Sections_ID","Credits_Attempted"]]

student_courses_date_courses = pd.merge( student_courses_date, courses, on=["Term","Course_Sections_ID"], how="left")

# %% Define generateSummary function
def generateSummary( df, suffix="" ):

    def enrollment_status( ch ):
        if ch >= 12:
            es = "FT"
        elif ch >= 9:
            es = "TQ"
        elif ch >= 6:
            es = "HT"
        elif ch > 0:
            es = "LH"
        else:
            es = "NE"

        return(es)

    # %% Add Seat_Type to STUDENT_ACAD_CRED date
    df.loc[df["Delivery_Method"] == "IN", "Seat_Type"] = "Online"
    df.loc[df["Delivery_Method"] != "IN", "Seat_Type"] = "Seated"

    df_ids = df.loc[df["Seat_Type"]=="Seated",["Student_ID"]].drop_duplicates()
    df_courses = pd.merge(df,df_ids,on="Student_ID",how="inner")
    df_courses.set_index(["Student_ID","Term"],inplace=True)

    # %% Generate summary for STUDENT_ACAD_CRED Date Seated 
    df_courses_seated = df_courses[df_courses["Seat_Type"]=="Seated"].copy().drop(["Seat_Type"], axis=1)
    df_courses_seated_group = df_courses_seated.groupby(level=df_courses_seated.index.names)
    df_courses_seated_summary = df_courses_seated_group.agg(
        Total_Credits_Attempted_Seated=pd.NamedAgg(column="Credits_Attempted", aggfunc="sum"),
        Total_Courses_Seated=pd.NamedAgg(column="Course_Sections_ID", aggfunc="count")
        )
    df_courses_seated_summary.reset_index(inplace=True)

    # %% Generate summary for STUDENT_ACAD_CRED Date Online 
    df_courses_online = df_courses[df_courses["Seat_Type"]=="Online"].copy().drop(["Seat_Type"], axis=1)
    df_courses_online_group = df_courses_online.groupby(level=df_courses_online.index.names)
    df_courses_online_summary = df_courses_online_group.agg(
        Total_Credits_Attempted_Online=pd.NamedAgg(column="Credits_Attempted", aggfunc="sum"),
        Total_Courses_Online=pd.NamedAgg(column="Course_Sections_ID", aggfunc="count")
        )
    df_courses_online_summary.reset_index(inplace=True)

    # %% Generate summary for STUDENT_ACAD_CRED Date
    df_courses_summary = pd.merge( df_courses_seated_summary, df_courses_online_summary, on=["Student_ID","Term"], how="left").replace(np.nan,0)
    df_courses_summary.reset_index(inplace=True)
    df_courses.reset_index(inplace=True)

    df_courses_summary["Total_Credits_Attempted"] = df_courses_summary["Total_Credits_Attempted_Seated"] + df_courses_summary["Total_Credits_Attempted_Online"]
    df_courses_summary["Total_Courses"] = df_courses_summary["Total_Courses_Seated"] + df_courses_summary["Total_Courses_Online"]
    df_courses_summary["Enrollment_Status"] = df_courses_summary.apply(lambda row: enrollment_status(row["Total_Credits_Attempted"]), axis=1)

    df_courses_summary.rename(columns={ "Total_Credits_Attempted_Online" : f"Total_Credits_Attempted_Online_{suffix}",
                                        "Total_Credits_Attempted_Seated" : f"Total_Credits_Attempted_Seated_{suffix}",
                                        "Total_Credits_Attempted"        : f"Total_Credits_Attempted_{suffix}",
                                        "Total_Courses_Online"           : f"Total_Courses_Online_{suffix}",
                                        "Total_Courses_Seated"           : f"Total_Courses_Seated_{suffix}",
                                        "Total_Courses"                  : f"Total_Courses_{suffix}",
                                        "Enrollment_Status"              : f"Enrollment_Status_{suffix}" },
                              inplace=True)
    return(df_courses_summary)

# %% Load STUDENT_ACAD_CRED for latest date
student_courses_cur_cols = { "STC.PERSON.ID"      : "Student_ID",
                             "STC.TERM"           : "Term",
                             "STC.COURSE"         : "Course_ID",
                             "STC.COURSE.SECTION" : "Course_Sections_ID",
                             "STC.CRED"           : "Credits_Attempted"
                            }
student_courses_cur_where = f"[STC.ACAD.LEVEL] = 'CU' AND [STC.TERM] = '{term}' AND [STC.STATUS] IN ('A','N')"
student_courses_cur = getColleagueData( engine, "STUDENT_ACAD_CRED", 
                                    cols = student_courses_cur_cols, 
                                    where = student_courses_cur_where
                                   )
student_courses_cur_courses = pd.merge( student_courses_cur, courses, on=["Term","Course_Sections_ID"], how="left")

# %% Generate summaries for STUDENT_ACAD_CRED
student_courses_date_summary = generateSummary( student_courses_date_courses, dt )
student_courses_cur_summary = generateSummary( student_courses_cur_courses, "End")

# %% Generate summary for STUDENT_ACAD_CRED
student_courses_summary = pd.merge(student_courses_date_summary,student_courses_cur_summary,on=["Student_ID","Term"],how="left").replace(np.nan,0)
student_courses_summary.drop(["Term","index_x","index_y"],axis=1,inplace=True)
student_courses_summary = student_courses_summary.loc[:,~student_courses_summary.columns.str.startswith('Total_Courses')]
student_courses_summary = student_courses_summary.loc[:,~student_courses_summary.columns.str.startswith('Total_Credits_Attempted_Online')]
student_courses_summary = student_courses_summary.loc[:,~student_courses_summary.columns.str.startswith('Total_Credits_Attempted_Seated')]

# %% Combine PERSON, FAFSA, and CS.ACYR data
person_fafsa = pd.merge(person, csacyr_fafsa, on="Student_ID", how='inner')

# %% Merge Person, FAFSA, and Student Course Summary
person_fafsa_student_courses_summary = pd.merge(person_fafsa, student_courses_summary, on="Student_ID", how="inner")
person_fafsa_student_courses_summary.to_csv(output_fn,na_rep='',index=False)

# student_courses_summary.to_csv("student_courses_summary.csv",index=False)
# person_fafsa.to_csv("person_fafsa.csv",index=False)

# %% End of Program
print('Done')
