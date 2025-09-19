# api/track_types.py
# M = Metro, P = Provincial, C = Country
# All matching case-insensitive; we canonicalize to UPPER for keys.

from typing import Tuple

_ALIAS_EQUIV = {
    # VIC
    ("VIC", "SANDOWN"): "SANDOWN",
    ("VIC", "SANDOWN HILLSIDE"): "SANDOWN HILLSIDE",
    ("VIC", "SANDOWN LAKESIDE"): "SANDOWN LAKESIDE",
    ("VIC", "WERRIBEE PARK"): "WERRIBEE",
    ("VIC", "CRANBOURNE TRN"): "CRANBOURNE",
    ("VIC", "SOUTHSIDE CRANBOURNE"): "CRANBOURNE",
    ("VIC", "SPORTSBET-BALLARAT"): "BALLARAT",
    ("VIC", "PICKLEBET PARK WODONGA"): "WODONGA",

    # NSW
    ("NSW", "RANDWICK KENSINGTON"): "KENSINGTON",
    ("NSW", "CANTERBURY PARK"): "CANTERBURY",
    ("NSW", "ROSEHILL"): "ROSEHILL GARDENS",

    # QLD
    ("QLD", "CALOUNDRA"): "SUNSHINE COAST",
    ("QLD", "SUNSHINE COAST INNER"): "SUNSHINE COAST",
    ("QLD", "AQUIS PARK GOLD COAST"): "GOLD COAST",

    # TAS
    ("TAS", "DEVONPORT TAPETA"): "DEVONPORT",
    ("TAS", "SPREYTON"): "DEVONPORT",

    # SA
    ("SA", "MURRAY BRIDGE GH"): "MURRAY BRIDGE",
    ("SA", "MURRAY BRIDGE PARK"): "MURRAY BRIDGE",
}

TRACK_GRADE = {
    # ACT
    ("ACT", "CANBERRA"): "P",

    # NSW — metro
    ("NSW", "RANDWICK"): "M",
    ("NSW", "KENSINGTON"): "M",
    ("NSW", "ROSEHILL GARDENS"): "M",
    ("NSW", "CANTERBURY"): "M",
    ("NSW", "WARWICK FARM"): "M",
    # NSW — provincial
    ("NSW", "GOSFORD"): "P",
    ("NSW", "HAWKESBURY"): "P",
    ("NSW", "KEMBLA GRANGE"): "P",
    ("NSW", "NEWCASTLE"): "P",
    ("NSW", "WYONG"): "P",
    # NSW — common country
    ("NSW", "NOWRA"): "C",
    ("NSW", "COFFS HARBOUR"): "C",
    ("NSW", "MUSWELLBROOK"): "C",
    ("NSW", "DUBBO"): "C",
    ("NSW", "TAREE"): "C",
    ("NSW", "SCONE"): "C",
    ("NSW", "GRAFTON"): "C",

    # VIC — metro
    ("VIC", "FLEMINGTON"): "M",
    ("VIC", "CAULFIELD"): "M",
    ("VIC", "MOONEE VALLEY"): "M",
    ("VIC", "SANDOWN"): "M",
    ("VIC", "SANDOWN HILLSIDE"): "M",
    ("VIC", "SANDOWN LAKESIDE"): "M",
    # VIC — provincial
    ("VIC", "BALLARAT"): "P",
    ("VIC", "BENDIGO"): "P",
    ("VIC", "CRANBOURNE"): "P",
    ("VIC", "GEELONG"): "P",
    ("VIC", "KILMORE"): "P",
    ("VIC", "KYNETON"): "P",
    ("VIC", "MOE"): "P",
    ("VIC", "MORNINGTON"): "P",
    ("VIC", "PAKENHAM"): "P",
    ("VIC", "SALE"): "P",
    ("VIC", "SEYMOUR"): "P",
    ("VIC", "WARRNAMBOOL"): "P",
    ("VIC", "WERRIBEE"): "P",
    ("VIC", "YARRA GLEN"): "P",
    ("VIC", "TRARALGON"): "P",
    # VIC — country examples
    ("VIC", "WANGARATTA"): "C",
    ("VIC", "WODONGA"): "C",
    ("VIC", "SWAN HILL"): "C",
    ("VIC", "STAWELL"): "C",
    ("VIC", "TERANG"): "C",
    ("VIC", "ECHUCA"): "C",

    # QLD — metro
    ("QLD", "DOOMBEN"): "M",
    ("QLD", "EAGLE FARM"): "M",
    # QLD — provincial
    ("QLD", "SUNSHINE COAST"): "P",
    ("QLD", "IPSWICH"): "P",
    ("QLD", "TOOWOOMBA"): "P",
    ("QLD", "GOLD COAST"): "P",
    # QLD — country
    ("QLD", "TOWNSVILLE"): "C",
    ("QLD", "ROCKHAMPTON"): "C",
    ("QLD", "MACKAY"): "C",
    ("QLD", "CAIRNS"): "C",
    ("QLD", "ROMA"): "C",

    # SA
    ("SA", "MORPHETTVILLE"): "M",
    ("SA", "BALAKLAVA"): "P",
    ("SA", "GAWLER"): "P",
    ("SA", "MURRAY BRIDGE"): "P",
    ("SA", "STRATHALBYN"): "P",
    ("SA", "PORT LINCOLN"): "C",
    ("SA", "NARACOORTE"): "C",
    ("SA", "PENOLA"): "C",
    ("SA", "MOUNT GAMBIER"): "C",

    # WA
    ("WA", "ASCOT"): "M",
    ("WA", "BELMONT"): "M",
    ("WA", "PINJARRA"): "P",
    ("WA", "NORTHAM"): "P",
    ("WA", "BUNBURY"): "P",
    ("WA", "YORK"): "P",
    ("WA", "KALGOORLIE"): "C",
    ("WA", "ESPERANCE"): "C",
    ("WA", "ALBANY"): "C",
    ("WA", "GERALDTON"): "C",

    # TAS
    ("TAS", "HOBART"): "M",
    ("TAS", "LAUNCESTON"): "M",
    ("TAS", "DEVONPORT"): "P",

    # NT
    ("NT", "DARWIN"): "M",
    ("NT", "ALICE SPRINGS"): "C",
    ("NT", "KATHERINE"): "C",
}

def _canon(s: str) -> str:
    return (s or "").strip().upper()

def canonicalize_track(state: str, track: str) -> Tuple[str, str]:
    st = _canon(state)
    tr = _canon(track)
    alt = _ALIAS_EQUIV.get((st, tr))
    if alt:
        tr = alt
    return st, tr

def get_track_type(state: str, track: str) -> str:
    st, tr = canonicalize_track(state, track)
    return TRACK_GRADE.get((st, tr)) or ""
