"""
src/common/dims.py — small static dimension tables, broadcast-joined onto big fact streams.

A broadcast hash join ships the small table to every executor once, so the
multi-million-row side is never shuffled.
"""

from pyspark.sql import DataFrame, SparkSession

# CICIDS2017 testbed topology (UNB): victim LAN 192.168.10.0/24, attacker/firewall 205.174.165.0/24
SUBNETS = [
    ("192.168.10", "internal", "Victim LAN"),
    ("205.174.165", "attacker_net", "Attacker / firewall segment"),
    ("172.16.0", "internal", "Firewall NAT side"),
]

PORTS = [
    (21, "FTP"), (22, "SSH"), (23, "Telnet"), (25, "SMTP"), (53, "DNS"), (80, "HTTP"),
    (110, "POP3"), (123, "NTP"), (137, "NetBIOS-NS"), (139, "NetBIOS-SSN"), (143, "IMAP"),
    (443, "HTTPS"), (445, "SMB"), (389, "LDAP"), (3268, "LDAP-GC"), (3389, "RDP"),
    (8080, "HTTP-alt"), (8443, "HTTPS-alt"), (444, "SNPP"), (88, "Kerberos"), (135, "MS-RPC"),
]


def dim_subnets(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(SUBNETS, "net_prefix string, zone string, zone_desc string")


def dim_ports(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(PORTS, "port int, service string")
