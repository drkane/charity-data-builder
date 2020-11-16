VIEWS = {
    "registration_basic": """
        select
            c.regno,
            reg.regdate as date_registered,
            rem.remdate as date_removed,
            remove_ref.text as reason_removed
        from
            charity c
            left outer join (
                select regno,
                    regdate from registration
                group by regno
                having rowid = min(rowid)
                order by regno, regdate
            ) as reg on c.regno = reg.regno
            left outer join (
                select
                    regno,
                    remdate,
                    remcode
                from
                    registration
                group by
                    regno
                having rowid = max(rowid)
                order by
                    regno asc,
                    regdate asc
            ) as rem on c.regno = rem.regno
            left outer join remove_ref
                on rem.remcode = remove_ref.code
    """,
    "ccew_main": """
select
  c.regno,
  c.name,
  c.orgtype,
  c.aob,
  c.postcode,
  mc.coyno,
  mc.fyend,
  mc.welsh == 'T' as welsh,
  mc.incomedate,
  mc.income,
  mc.web,
  r.date_registered as date_registered,
  r.date_removed as date_removed,
  r.reason_removed as reason_removed
from
  charity c
  left outer join main_charity mc on c.regno = mc.regno
  left outer join registration_basic r on c.regno = r.regno
where c.orgtype == 'R'
    """
}
