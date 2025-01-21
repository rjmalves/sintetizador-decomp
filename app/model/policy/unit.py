from enum import Enum


class Unit(Enum):
    RS = "R$"
    kRS = "10^3 R$"
    kRS_MWh = "10^3 R$/MWh"
    kRS_hm3 = "10^3 R$/hm3"
    MWh = "MWh"
    hm3 = "hm3"
