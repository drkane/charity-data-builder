VIEWS = {
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
            r.regdate as date_registered,
            r.remdate as date_removed,
            remove_ref.text as reason_removed
        from
            charity c
                left outer join main_charity mc on c.regno = mc.regno
                left outer join registration r on c.regno = r.regno
                left outer join remove_ref
                    on r.remcode = remove_ref.code
        where c.orgtype == 'R'
    """,
    "ccew_removed": """
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
            r.regdate as date_registered,
            r.remdate as date_removed,
            remove_ref.text as reason_removed
        from
            charity c
                left outer join main_charity mc on c.regno = mc.regno
                left outer join registration r on c.regno = r.regno
                left outer join remove_ref
                    on r.remcode = remove_ref.code
        where c.orgtype == 'RM'
    """,
    "legal_form": """
        select charity.regno,
            name,
            case when gd like 'CIO - Found%' then 'CIO - Foundation'
                 when gd like 'CIO - Assoc%' then 'CIO - Association'
                 when main_charity.coyno then 'Charitable Company'
                 else 'Unincorporated charity' end as legal_form,
            gd as governing_document
        from charity left outer join main_charity on charity.regno = main_charity.regno
        where charity.orgtype = 'R'
    """,
    "by_registration_year": """
select
  reg.year,
  ifnull(charities_registered, 0),
  ifnull(charities_removed, 0),
  (
    SUM(ifnull(charities_registered, 0)) OVER (ROWS UNBOUNDED PRECEDING) - SUM(ifnull(charities_removed, 0)) OVER (ROWS UNBOUNDED PRECEDING)
  ) as charities_total
from
  (
    select
      strftime('%Y', regdate) as year,
      count(*) as charities_registered
    from
      registration
    group by
      year
  ) as reg
  left outer join (
    select
      strftime('%Y', remdate) as year,
      count(*) as charities_removed
    from
      registration
    group by
      year
  ) as rem on reg.year = rem.year
    """
}
