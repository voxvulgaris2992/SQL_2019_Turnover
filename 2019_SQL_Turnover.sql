create table #calc
(
    fundname varchar ( 100 ),
    userreference varchar ( 100 ),
    avgturnover
        float ,
    stdevturnover float ,
    ASDP float ,
    ASP float
)
insert into #calc
    ( fundname , userreference , avgturnover , stdevturnover , ASDP , ASP )
select fundname , userreference , avg ( turnover ) as avgturnover , stdevp ( turnover ) as
stdevturnover ,
    ( 1.75 + 0.5 *( cast (( row_number () over ( order by stdevp ( turnover ))) as float )/( select
        count ( distinct fundname )
    from dbo . FinalResults_ToDelete_2017Jan01_2019Mar04 ))) ASDP ,
    --ASDP = adaptive standard deviation parameter
    ( 0.75 + 0.1 *( cast (( row_number () over ( order by stdevp ( turnover ))) as float )/( select
        count ( distinct fundname )
    from dbo . FinalResults_ToDelete_2017Jan01_2019Mar04 ))) ASP
--ASP = adaptive smoothing parameter
from dbo . FinalResults_ToDelete_2017Jan01_2019Mar04
--changing variable (found elsewhere too)
group by fundname , userreference
create table #past
(
    fundname varchar ( 100 ),
    userreference varchar ( 100 ),
    uploadedby
        varchar ( 100 ),
    snapshotdate varchar ( 100 ),
    prev float ,
    prevprev float
)
insert into #past
    ( fundname , userreference , uploadedby , snapshotdate , prev , prevprev )
select fundname , userreference , uploadedby , snapshotdate ,
    isnull ( LAG ( turnover ) over ( partition by
dbo . FinalResults_ToDelete_2017Jan01_2019Mar04 . uploadedby ,
dbo . FinalResults_ToDelete_2017Jan01_2019Mar04 . fundname
order by
dbo . FinalResults_ToDelete_2017Jan01_2019Mar04 . SnapshotDate ),( isnull ( LAG ( turnover , 2 )
over ( partition by dbo . FinalResults_ToDelete_2017Jan01_2019Mar04 . uploadedby ,
dbo . FinalResults_ToDelete_2017Jan01_2019Mar04 . fundname
order by
dbo . FinalResults_ToDelete_2017Jan01_2019Mar04 . SnapshotDate ),( isnull ( LAG ( turnover , 3 )
over ( partition by dbo . FinalResults_ToDelete_2017Jan01_2019Mar04 . uploadedby ,
dbo . FinalResults_ToDelete_2017Jan01_2019Mar04 . fundname
order by dbo . FinalResults_ToDelete_2017Jan01_2019Mar04 . SnapshotDate ), 0 ))))) as prev ,
    --recursive isnull function gives lag2 if lag1 is 0, given a range of 3 snapshotdates
    (lag1
to lag3)
isnull
( LAG
( turnover , 2 ) over
( partition by
dbo . FinalResults_ToDelete_2017Jan01_2019Mar04 . uploadedby ,
dbo . FinalResults_ToDelete_2017Jan01_2019Mar04 . fundname
order by
dbo . FinalResults_ToDelete_2017Jan01_2019Mar04 . SnapshotDate ),
( isnull
( LAG
( turnover , 3 )
over
( partition by dbo . FinalResults_ToDelete_2017Jan01_2019Mar04 . uploadedby ,
dbo . FinalResults_ToDelete_2017Jan01_2019Mar04 . fundname
order by
dbo . FinalResults_ToDelete_2017Jan01_2019Mar04 . SnapshotDate ),
( isnull
( LAG
( turnover , 4 )
over
( partition by dbo . FinalResults_ToDelete_2017Jan01_2019Mar04 . uploadedby ,
dbo . FinalResults_ToDelete_2017Jan01_2019Mar04 . fundname
order by dbo . FinalResults_ToDelete_2017Jan01_2019Mar04 . SnapshotDate ), 0 ))))) as prevprev
--recursive isnull function gives lag3 if lag2 is 0, given a range of 3 snapshotdates
(lag2 to lag4)
from dbo . FinalResults_ToDelete_2017Jan01_2019Mar04
group by fundname , userreference , uploadedby , snapshotdate , turnover
select uploadedby , fundname , userreference , snapshot , turnover , max ( madiff ) as
distinctflag
--distinctflag filters for results where turnovers exceed moving averages:
from ( select db . uploadedby , db . fundname , db . userreference , db . snapshot , db . turnover ,
        --their only significance is in being >0
        db . turnover -( avgturnover + ASDP *(( ASP * stdevturnover )+ prev *(( 1 - ASP )/ 2 )+ prevprev *(( 1 - ASP )/ 2
))) as madiff
    from dbo . FinalResults_ToDelete_2017Jan01_2019Mar04 db
        inner join #calc on #calc . fundname = db . fundname
        inner join #past on #past . fundname = db . fundname
    where snapshot = 'Jan-2019' --specify snapshot
        and
        db . turnover -( avgturnover + ASDP *(( ASP * stdevturnover )+ prev *(( 1 - ASP )/ 2 )+ prevprev *(( 1 - ASP )/ 2
)))> 0 ) a
group by uploadedby , fundname , userreference , snapshot , turnover
drop table #calc
drop table #past