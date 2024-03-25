import subprocess
import json
from dataclasses import dataclass
from typing import List, Dict, Tuple, Optional
import numpy as np
from itertools import chain

"""
Suggest optimizations for balancing Ceph OSDs across hosts.

The program analyzes the distribution of Ceph Object Storage Daemons (OSDs)
across different hosts within a Ceph storage cluster.
It then suggests how to balance the storage pool by recommending pairs of OSDs to
be swapped manually, one pair at a time.

The tool does not perform any actual swap operations; it only provides
recommendations based on the current state of the cluster. The suggestions are
calculated using the standard deviation of total storage sizes by OSD type
across all hosts, aiming to minimize this metric for a balanced configuration.
"""

@dataclass
class CephOsd:
    id: int
    bluestore_bdev_size: int
    device_ids: str
    hostname: str
    bluestore_bdev_type: str
    

def get_ceph_osd_metadata() -> List[CephOsd]:
    try:
        result = subprocess.run(['ceph', 'osd', 'metadata'], capture_output=True, text=True, check=True)
        osd_metadata = json.loads(result.stdout)

        osd_objects = []        
        for osd in osd_metadata:
            osd_obj = CephOsd(
                id=int(osd.get('id', 0)),
                bluestore_bdev_size=int(osd.get('bluestore_bdev_size', 0)),
                device_ids=osd.get('device_ids', ''),
                hostname=osd.get('hostname', ''),
                bluestore_bdev_type=osd.get('bluestore_bdev_type', '')
            )
            osd_objects.append(osd_obj)
        
        return osd_objects
    except subprocess.CalledProcessError as e:
        print(f"Error executing ceph osd metadata: {e}")
        return []


@dataclass
class CephHost:
    hostname: str
    osd_by_type: Dict[str, List[CephOsd]]


def group_osds_by_host_and_type(osds: List[CephOsd]) -> List[CephHost]:
    hosts: Dict[str, Dict[str, List[CephOsd]]] = {}
    for osd in osds:
        if osd.hostname not in hosts:
            hosts[osd.hostname] = {}
        hosts[osd.hostname].setdefault(osd.bluestore_bdev_type, []).append(osd)
    ceph_hosts = [CephHost(hostname=host, osd_by_type=types) for host, types in hosts.items()]    
    return ceph_hosts


def calculate_spread_by_type(hosts: List[CephHost]) -> Dict[str, float]:
    """Calculate spread (std deviation) of total sizes (in GB) by OSD type across all hosts."""
    total_sizes_by_type: Dict[str, List[int]] = {}
    for host in hosts:
        for osd_type, osds in host.osd_by_type.items():
            total_sizes_by_type.setdefault(osd_type, []).append(sum(osd.bluestore_bdev_size for osd in osds) // (10**9))
    return {osd_type: float(np.std(sizes)) for osd_type, sizes in total_sizes_by_type.items()}


def swap_osd(hosts: List[CephHost], osd_a: CephOsd, osd_b: CephOsd):
    """Swap two OSDs between hosts."""
    host_a = next(host for host in hosts if host.hostname == osd_a.hostname)
    host_b = next(host for host in hosts if host.hostname == osd_b.hostname)
    
    host_a.osd_by_type[osd_a.bluestore_bdev_type].remove(osd_a)
    host_b.osd_by_type[osd_b.bluestore_bdev_type].remove(osd_b)

    osd_a.hostname, osd_b.hostname = osd_b.hostname, osd_a.hostname    
    
    host_a.osd_by_type[osd_b.bluestore_bdev_type].append(osd_b)
    host_b.osd_by_type[osd_a.bluestore_bdev_type].append(osd_a)


def find_best_swap(hosts: List[CephHost]) -> Optional[Tuple[CephOsd, CephOsd]]:
    """Find the best pair of OSDs to swap between hosts to improve balance."""
    best_swap = None
    highest_spread_improvement = -float('inf')
    original_spread_by_type = calculate_spread_by_type(hosts)
    
    for host_a in hosts:
        for host_b in hosts:
            if host_a.hostname == host_b.hostname:
                continue

            # Get all OSDs from both hosts
            host_a_osds = list(chain(*host_a.osd_by_type.values()))
            host_b_osds = list(chain(*host_b.osd_by_type.values()))

            for osd_a in host_a_osds:
                for osd_b in host_b_osds:

                    # Skip trivially non-beneficial swaps
                    if (osd_a.bluestore_bdev_size == osd_b.bluestore_bdev_size) and (osd_a.bluestore_bdev_type == osd_b.bluestore_bdev_type):
                        continue

                    # Swap sizes hypothetically
                    swap_osd(hosts, osd_a, osd_b)

                    # Recalculate total sizes and spread
                    new_spread_by_type = calculate_spread_by_type(hosts)

                    # Calculate spread improvement for all types
                    improvements_per_type = list(original_spread_by_type[type] - new_spread_by_type[type] for type in original_spread_by_type.keys())
                    total_spread_improvement = sum(improvements_per_type)
                    all_spreads_not_increased = all(imp>=0 for imp in improvements_per_type)

                    if total_spread_improvement > 0 and total_spread_improvement > highest_spread_improvement and all_spreads_not_increased:
                        best_swap = (osd_a, osd_b)
                        highest_spread_improvement = total_spread_improvement

                    # Undo the hypothetical swap for the next iteration
                    swap_osd(hosts, osd_a, osd_b)

    return best_swap


def print_state(hosts: List[CephHost]):
    """Print the current state of the cluster."""
    for host in sorted(hosts, key=lambda h: h.hostname):
        print(f"{host.hostname}")
        for osd_type, osds in sorted(host.osd_by_type.items()):
            osd_strs = [f"osd.{osd.id} {osd.bluestore_bdev_size//(10**9)}GB" for osd in osds]
            total_size = sum(osd.bluestore_bdev_size for osd in osds) // (10**9)
            print(f"  {osd_type.upper()} (total {total_size} GB): {', '.join(osd_strs)}")


def optimize(hosts: List[CephHost]):
    """Simulate the optimization process and print the steps."""
    print("---------- Initial state:")
    print_state(hosts)
    print("")

    print("---------- Balancing steps:")
    i = 1
    while True:
        best_swap = find_best_swap(hosts)
        if not best_swap:
            break
    
        osd_a, osd_b = best_swap
        print(f"Swap #{i}:")
        print(f"  osd.{osd_a.id} @ {osd_a.hostname}, device: {osd_a.device_ids}, ({osd_a.bluestore_bdev_size//(10**9)}GB {osd_a.bluestore_bdev_type})")
        print("  <->")
        print(f"  osd.{osd_b.id} @ {osd_b.hostname}, device: {osd_b.device_ids}, ({osd_b.bluestore_bdev_size//(10**9)}GB {osd_b.bluestore_bdev_type})")
        print("")

        swap_osd(hosts, best_swap[0], best_swap[1])
        i += 1

    print("")
    print("---------- Final state:")
    print_state(hosts)        


if __name__ == "__main__":
    osds = get_ceph_osd_metadata()
    hosts = group_osds_by_host_and_type(osds)
    optimize(hosts)
