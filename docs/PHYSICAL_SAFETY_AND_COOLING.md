# Zeer Big Data Cluster — Physical Safety & Cooling Guide

**Version:** 1.0  
**Date:** 2026-09-22  
**Cluster:** 3× DataNode (LGA 1155) + 1× Master Node (Laptop)  
**Environment:** Home / Residential

---

## 1. Executive Summary

This document defines physical safety, thermal management, and environmental protection requirements for a 4-node Big Data cluster deployed on legacy LGA 1155 hardware (Intel Core i7-2600/3770 or Xeon E3-1275 v1/v2) in residential conditions. The cluster operates 24/7 under sustained compute load (Hadoop, Spark, Kafka, ZooKeeper).

**Key Constraints:**
- TDP: 77–95W per CPU (Sandy/Ivy Bridge)
- 4 cores / 8 threads, sustained 100% load
- 16 GB DDR3 RAM (2×8 GB)
- 512 GB SATA SSD/HDD
- Passive/active cooling in enclosed spaces
- Ambient temperature range: +5°C … +30°C (winter/summer)

---

## 2. Thermal Design & Cooling Architecture

### 2.1 Thermal Envelope Analysis

| Component | TDP / Heat Load | Cooling Requirement |
|-----------|-----------------|---------------------|
| CPU (i7-2600/Xeon E3-1275 v1) | 95 W | ≥120 W cooler |
| CPU (i7-3770/Xeon E3-1275 v2) | 77 W | ≥100 W cooler |
| VRM / Chipset | ~15–20 W | Airflow across heatsinks |
| RAM (2×8 GB DDR3) | ~6–8 W | Passive + case flow |
| SSD/HDD | ~5–8 W | Direct airflow |
| PSU (80+ Bronze 350W) | ~30 W loss | Self-cooled, exhaust |

**Total per node:** ~140–160 W sustained heat dissipation

### 2.2 CPU Cooler Selection (Mandatory)

| CPU Generation | TDP | Minimum Cooler Rating | Recommended Models |
|----------------|-----|----------------------|-------------------|
| Sandy Bridge (i7-2600, Xeon E3-1275 v1) | 95 W | **120–130 W** | ID-COOLING SE-214-XT (4 heatpipes, 120mm), Deepcool AK620, Thermalright Assassin X 120 |
| Ivy Bridge (i7-3770, Xeon E3-1275 v2) | 77 W | **100–120 W** | ID-COOLING SE-903-XT (3 heatpipes, 92mm), Deepcool GAMMAXX 400, Arctic Freezer 34 eSports |

**Installation Requirements:**
- ✅ Clean CPU IHS + cooler base with isopropyl alcohol (99%)
- ✅ Apply **pea-sized dot** of thermal paste (NT-H2, MX-6, GD900, PTM7950)
- ✅ Mount with **even pressure** — diagonal tightening sequence
- ✅ Verify fan header: **CPU_FAN** (PWM, 4-pin)
- ✅ Set BIOS fan curve: **40% @ 40°C → 100% @ 70°C**

### 2.3 Case Airflow Design (Positive Pressure)

```
┌─────────────────────────────────────────────────────┐
│                    FRONT (Intake)                    │
│  [2×120mm or 3×120mm]  ──────────────────────►      │
│     FILTERED              │   │   │                  │
│                           ▼   ▼   ▼                  │
│              ┌─────────────────────────────┐        │
│              │  HDD/SSD  │  RAM  │  VRM   │        │
│              │  (bracket)│       │  heats │        │
│              └─────────────────────────────┘        │
│                           │   │   │                  │
│                           ▼   ▼   ▼                  │
│                    CPU COOLER (tower)                │
│                         │   │                        │
│                         ▼   ▼                        │
│              [1×120mm REAR EXHAUST]  ◄──────────────┘
│                         │                           │
│                    PSU (exhaust)                    │
└─────────────────────────────────────────────────────┘
```

**Fan Configuration (per node):**
| Position | Size | Count | RPM Range | Role |
|----------|------|-------|-----------|------|
| Front intake | 120mm | 2–3 | 800–1200 | Filtered intake |
| Rear exhaust | 120mm | 1 | 800–1400 | Primary exhaust |
| Top exhaust | 120mm | 1 (opt) | 600–1000 | Hot air rise assist |
| CPU cooler | 120mm | 1–2 | 600–1800 | PWM, CPU temp controlled |

**Airflow Target:** **Positive pressure** (intake CFM > exhaust CFM) — prevents dust ingress through gaps.

**Dust Filters:** **Mandatory** on all intakes. Clean monthly.

### 2.4 BIOS/UEFI Fan Curve Configuration

```text
CPU_FAN (PWM):
  30°C → 30% (silent idle)
  45°C → 50%
  60°C → 75%
  75°C → 100% (max cooling)

SYS_FAN1 (Front):
  30°C → 40%
  45°C → 60%
  60°C → 85%

SYS_FAN2 (Rear):
  30°C → 50%
  45°C → 70%
  60°C → 95%
```

**Critical:** Disable "Smart Fan" / "Quiet Mode" — use **manual curves** tied to **CPU package temperature** (not socket temp).

---

## 3. Overheating Protection (Multi-Layer)

### 3.1 Hardware-Level Protection

| Layer | Mechanism | Trigger | Action |
|-------|-----------|---------|--------|
| **L1: CPU Internal** | Thermal Monitor (TM1/TM2) | 100°C (Sandy) / 105°C (Ivy) | Clock modulation / shutdown |
| **L2: BIOS** | Critical Temp Shutdown | 90–95°C (configurable) | Hard power off |
| **L3: OS (lm-sensors)** | `thermal_zone0` / `coretemp` | 80°C warn / 85°C critical | Alert → throttling → shutdown |
| **L4: Application** | Custom watchdog (Node.js) | 80°C sustained 5 min | Graceful service drain + shutdown |

### 3.2 Linux Thermal Monitoring Setup (All Nodes)

```bash
# Install monitoring
sudo apt install -y lm-sensors hddtemp psutil python3-psutil

# Detect sensors
sudo sensors-detect --auto

# Verify
sensors
# Expected output:
# coretemp-isa-0000
# Package id 0:  +45.0°C  (high = +80.0°C, crit = +95.0°C)
# Core 0:        +42.0°C
# Core 1:        +44.0°C
# Core 2:        +43.0°C
# Core 3:        +41.0°C
```

### 3.3 Automated Thermal Watchdog (systemd Service)

**File:** `/etc/systemd/system/thermal-watchdog.service`
```ini
[Unit]
Description=Zeer Thermal Watchdog
After=network.target
Wants=network.target

[Service]
Type=simple
User=hadoop
ExecStart=/opt/zeer/scripts/thermal-watchdog.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

**File:** `/opt/zeer/scripts/thermal-watchdog.py`
```python
#!/usr/bin/env python3
"""
Zeer Big Data Thermal Watchdog
Monitors CPU, RAM, disk temps. Triggers graceful degradation.
"""
import psutil
import time
import subprocess
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('/opt/zeer/logs/thermal-watchdog.log'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

THRESHOLDS = {
    'cpu_warn': 75,      # °C
    'cpu_crit': 85,      # °C
    'cpu_emergency': 92, # °C - immediate shutdown
    'disk_warn': 55,     # °C
    'disk_crit': 60,     # °C
    'ambient_warn': 35,  # °C (estimated via CPU idle delta)
}

ACTION_COOLDOWN = 300  # seconds between actions

def get_cpu_temps():
    """Return list of per-core temps + package temp"""
    temps = psutil.sensors_temperatures()
    core_temps = []
    pkg_temp = None
    
    for name, entries in temps.items():
        if 'coretemp' in name or 'k10temp' in name:
            for entry in entries:
                if 'Package' in entry.label or 'package' in entry.label:
                    pkg_temp = entry.current
                elif 'Core' in entry.label:
                    core_temps.append(entry.current)
    return core_temps, pkg_temp

def get_disk_temps():
    """Return dict of disk temps via hddtemp/smartctl"""
    temps = {}
    try:
        import subprocess
        out = subprocess.check_output(['smartctl', '-A', '/dev/sda'], text=True)
        for line in out.split('\n'):
            if 'Temperature_Celsius' in line or 'Temperature' in line:
                parts = line.split()
                temps['sda'] = int(parts[-1])
    except Exception:
        pass
    return temps

def trigger_throttling():
    """Reduce CPU frequency via cpufreq"""
    try:
        subprocess.run(['cpupower', 'frequency-set', '-u', '2.0GHz'], check=False)
        subprocess.run(['cpupower', 'frequency-set', '-g', 'powersave'], check=False)
        log.warning("CPU throttled to 2.0GHz powersave mode")
    except Exception as e:
        log.error(f"Throttle failed: {e}")

def trigger_graceful_shutdown():
    """Drain services and shutdown"""
    log.critical("EMERGENCY: Thermal limit exceeded. Initiating graceful shutdown.")
    # Stop Flume first (data ingestion)
    subprocess.run(['systemctl', 'stop', 'zeer-flume'], check=False)
    # Stop Spark workers
    subprocess.run(['systemctl', 'stop', 'zeer-spark-worker'], check=False)
    # Stop Kafka
    subprocess.run(['systemctl', 'stop', 'zeer-kafka'], check=False)
    # Stop YARN NM
    subprocess.run(['systemctl', 'stop', 'hadoop-yarn-nodemanager'], check=False)
    # Stop HDFS DN
    subprocess.run(['systemctl', 'stop', 'hadoop-hdfs-datanode'], check=False)
    time.sleep(10)
    subprocess.run(['systemctl', 'poweroff'], check=False)

def main():
    last_action = 0
    consecutive_crit = 0
    
    log.info("=== Thermal Watchdog Started ===")
    
    while True:
        try:
            core_temps, pkg_temp = get_cpu_temps()
            disk_temps = get_disk_temps()
            
            max_core = max(core_temps) if core_temps else 0
            max_disk = max(disk_temps.values()) if disk_temps else 0
            
            log.debug(f"CPU: pkg={pkg_temp}°C max_core={max_core}°C | Disk: {disk_temps}")
            
            now = time.time()
            
            # Emergency shutdown
            if pkg_temp and pkg_temp >= THRESHOLDS['cpu_emergency']:
                trigger_graceful_shutdown()
                break
            
            # Critical - throttle + alert
            if pkg_temp and pkg_temp >= THRESHOLDS['cpu_crit']:
                consecutive_crit += 1
                if now - last_action > ACTION_COOLDOWN:
                    trigger_throttling()
                    # Send alert via Node.js API
                    subprocess.run([
                        'curl', '-s', '-X', 'POST',
                        'http://zeer-master:3000/api/cluster/alert',
                        '-H', 'Content-Type: application/json',
                        '-d', f'{{"type":"thermal","severity":"critical","cpu_temp":{pkg_temp}}}'
                    ], check=False)
                    last_action = now
            else:
                consecutive_crit = 0
            
            # Warning - alert only
            if pkg_temp and pkg_temp >= THRESHOLDS['cpu_warn'] and now - last_action > ACTION_COOLDOWN:
                subprocess.run([
                    'curl', '-s', '-X', 'POST',
                    'http://zeer-master:3000/api/cluster/alert',
                    '-H', 'Content-Type: application/json',
                    '-d', f'{{"type":"thermal","severity":"warning","cpu_temp":{pkg_temp}}}'
                ], check=False)
                last_action = now
            
            # Disk thermal check
            for disk, temp in disk_temps.items():
                if temp >= THRESHOLDS['disk_crit'] and now - last_action > ACTION_COOLDOWN:
                    log.warning(f"Disk {disk} critical temp: {temp}°C")
                    last_action = now
                    
        except Exception as e:
            log.error(f"Watchdog error: {e}")
        
        time.sleep(30)  # Check every 30 seconds

if __name__ == '__main__':
    main()
```

**Deploy:**
```bash
sudo cp thermal-watchdog.py /opt/zeer/scripts/
sudo chmod +x /opt/zeer/scripts/thermal-watchdog.py
sudo systemctl daemon-reload
sudo systemctl enable --now thermal-watchdog
```

---

## 4. Electrical Safety & Short Circuit Prevention

### 4.1 Power Supply Requirements

| Parameter | Specification | Rationale |
|-----------|---------------|-----------|
| **Wattage** | 350–400W (80+ Bronze min) | 150W peak load + 50% headroom |
| **Efficiency** | 80+ Bronze / Gold | Less waste heat, stable voltages |
| **Rail** | Single +12V rail preferred | Simpler load distribution |
| **Protections** | OVP, UVP, OCP, OPP, SCP | Mandatory for 24/7 operation |
| **Brands** | Seasonic, FSP, Chieftec, Deepcool, Corsair, be quiet! | No generic/unknown brands |

**Connection Rules:**
- ✅ **Direct wall outlet** — no power strips, no daisy-chaining
- ✅ **Surge protector** (MOV-based, >2000J) between wall and PSU
- ✅ **UPS** (optional but recommended): 600–800VA pure sine wave per node
- ✅ **Grounded outlet** (3-prong) — verify with socket tester

### 4.2 Cable Management & Short Prevention

```
PSU → 24-pin ATX          → Motherboard (fully seated, latch clicked)
PSU → 4/8-pin CPU (EPS)   → Motherboard (near CPU socket)
PSU → SATA Power          → SSD/HDD (straight, no sharp bends)
PSU → SATA Power          → Case fans (if not on mobo)
Motherboard → SATA Data   → SSD/HDD (latching connectors)
Case → F_PANEL            → Motherboard (PWR_SW, RST_SW, LEDs)
Case → USB 2.0/3.0        → Motherboard headers
Case → HD Audio           → Motherboard header
```

**Anti-Short Checklist:**
- [ ] Motherboard on **standoffs** (no direct case contact)
- [ ] No loose screws under motherboard
- [ ] All power connectors **fully seated** (audible click)
- [ ] No exposed wire strands at connectors
- [ ] SATA data cables: **latching type only**
- [ ] GPU/PCIe: none (using iGPU) — slot covers installed
- [ ] PSU voltage switch (if present): **230V** (EU/RU)

### 4.3 Grounding & ESD Protection

| Measure | Implementation |
|---------|----------------|
| **Chassis Ground** | PSU grounded via 3-prong plug → case → mobo standoffs |
| **Anti-Static Mat** | Use during assembly/maintenance |
| **Wrist Strap** | Wear when handling components |
| **Humidity** | Maintain 40–60% RH (reduces ESD risk) |
| **Flooring** | Avoid carpet; use anti-static mat or hard floor |

---

## 5. Overcooling / Condensation Protection (Winter)

### 5.1 The Condensation Risk

**Physics:** When hardware cools below **dew point**, moisture condenses on components → short circuits, corrosion.

| Ambient | RH | Dew Point | Risk If Hardware < |
|---------|-----|-----------|-------------------|
| +5°C | 60% | -1°C | Low (hardware > ambient) |
| +10°C | 70% | +4°C | Medium |
| +15°C | 80% | +11°C | **HIGH** |
| +20°C | 60% | +12°C | **HIGH** |

**Danger Zone:** Hardware powered **OFF** in cold room → cools to ambient → powered **ON** → warm moist air hits cold surfaces.

### 5.2 Mandatory Winter Protocols

#### A. Never Cold-Start Below +10°C
```bash
# Pre-start warmup (run on master via SSH)
for node in zeer-worker1 zeer-worker2 zeer-worker3; do
    ssh hadoop@$node "echo 'Pre-warming...'; sleep 300" &
done
# Wait 5 minutes with fans at low speed before full boot
```

#### B. Maintain Minimum Ambient Temperature
- **Space heater** (ceramic, 500–1000W) with thermostat set to **+12°C minimum**
- **Enclosed rack/cabinet** with small heater → creates stable microclimate
- **Insulate** bottom of case from cold floor (rubber feet + foam pad)

#### C. Power-On Sequence (Winter)
1. **Heater ON** → wait until ambient ≥ +12°C
2. **PSU switch ON** (standby power warms caps)
3. Wait **2 minutes** (caps charge, board warms slightly)
4. **Power button** → boot
5. Fans run at **minimum 30%** for first 10 minutes (BIOS curve)

#### D. Condensation Barrier (Conformal Coating - Optional but Recommended)
For extreme conditions (unheated garage/basement):
- **MG Chemicals 422B** or **Electrolube DCA** on:
  - Motherboard (both sides, avoid sockets/slots)
  - RAM sticks (edges only)
  - SSD/HDD PCB
- **Dielectric grease** on:
  - CPU socket (LGA pins) — **tiny amount**
  - PCIe / RAM slots — **tiny amount**
  - Fan headers

---

## 6. Physical Security & Mounting

### 6.1 Placement Rules

| Requirement | Specification |
|-------------|---------------|
| **Surface** | Stable, level, vibration-free (desk, shelf, rack) |
| **Clearance** | ≥15 cm rear, ≥10 cm sides, ≥20 cm front |
| **Floor** | NO direct floor contact — use rubber feet / casters / shelf |
| **Wall** | ≥10 cm from wall (rear exhaust) |
| **Stacking** | **NEVER** stack nodes — use vertical rack or separate shelves |
| **Orientation** | Motherboard vertical (standard tower) — not horizontal |

### 6.2 Vibration & Shock Protection

- **HDD nodes:** Use **silicone grommets** or **suspension mounts** for drive cages
- **SSD nodes:** Standard mounting OK (no moving parts)
- **Anti-vibration pads** under case feet (rubber/silicone)
- **Cable strain relief:** Velcro ties, no tension on connectors

### 6.3 Fire Safety

| Measure | Implementation |
|---------|----------------|
| **Extinguisher** | CO₂ or ABC powder (2kg) within 3m |
| **Smoke Detector** | Photoelectric, ceiling-mounted above cluster |
| **Cable Rating** | All cables: **VW-1 / FT1** flame rated |
| **PSU** | Metal enclosure, no paper labels near exhaust |
| **No Flammables** | Zero paper, cardboard, fabrics within 50 cm |

---

## 7. Environmental Monitoring (IoT Layer)

### 7.1 Recommended Sensors (Per Rack/Room)

| Sensor | Model | Interface | Placement |
|--------|-------|-----------|-----------|
| **Temp/Humidity** | Aqara WSDCGQ11LM / Sonoff SNZB-02D | Zigbee / WiFi | Center of cluster, 1m height |
| **Power** | Shelly PM Mini / Sonoff POW R3 | WiFi / MQTT | Each node PSU input |
| **Airflow** | Custom (Arduino + F300) | MQTT | Exhaust rear of each node |
| **Water Leak** | Aqara SJCGQ11LM | Zigbee | Floor under cluster |

### 7.2 Integration with Node.js API

**Endpoint:** `POST /api/cluster/environmental`
```json
{
  "node": "zeer-worker1",
  "ambient_temp": 22.5,
  "ambient_humidity": 45,
  "power_watts": 142,
  "airflow_rpm": 1100,
  "water_leak": false
}
```

**Alerting Rules:**
- Ambient > 28°C → Warning
- Ambient < 10°C → Critical (heater check)
- Humidity > 70% → Warning (condensation risk)
- Power > 180W sustained → Warning (anomaly)
- Airflow < 600 RPM → Critical (fan failure)

---

## 8. Maintenance Schedule

| Frequency | Task | Responsible |
|-----------|------|-------------|
| **Daily** | Check thermal watchdog logs, verify all nodes alive | Automated |
| **Weekly** | Visual inspection: dust, fan spin, cable integrity | Operator |
| **Monthly** | Clean dust filters, vacuum case interior (compressed air) | Operator |
| **Quarterly** | Re-paste CPU (if >70°C sustained), check PSU fan | Operator |
| **Semi-Annual** | Full disassembly, clean, inspect caps, test UPS | Operator |
| **Annual** | Replace thermal paste, verify BIOS settings, test shutdown | Operator |

### 8.1 Dust Cleaning Procedure
1. **Power off** → unplug PSU → hold power button 10s (drain caps)
2. **Compressed air** (upright can, short bursts) — fans held still
3. **Anti-static brush** for heatsink fins
4. **Vacuum** (ESD-safe) for loose dust — NO vacuum on components
5. **Verify** all fans spin freely before power on

---

## 9. Emergency Procedures

### 9.1 Thermal Runaway
```
1. Watchdog triggers → logs alert → throttles CPU
2. If temp > 92°C → graceful service drain → shutdown
3. Alert sent to master API → Telegram/Email notification
4. DO NOT restart until root cause found (dust, fan fail, ambient)
```

### 9.2 Power Event (Outage/Surge)
```
1. UPS kicks in (if present) → graceful shutdown on low battery
2. Without UPS: abrupt power loss → fsck on next boot
3. Post-power: verify all nodes boot, HDFS/YARN recover
4. Run: hdfs fsck / && yarn node -list
```

### 9.3 Water/Leak Event
```
1. Leak sensor triggers → immediate power cut (smart relay)
2. Alert to operator
3. Dry 24h minimum before power restore
4. Inspect for corrosion before boot
```

---

## 10. Validation Checklist (Pre-Production)

### Thermal Validation (Run 4h Stress Test)
```bash
# On each worker node
sudo apt install -y stress-ng
stress-ng --cpu 8 --cpu-load 100 --timeout 4h --metrics-brief
# Monitor: sensors every 30s, log to file
# PASS: Package temp < 80°C sustained, no throttling
```

### Electrical Validation
- [ ] All outlets grounded (tester shows OK)
- [ ] Surge protector LED green
- [ ] PSU voltages stable (HWMonitor: +12V 11.9–12.1, +5V 4.9–5.1, +3.3V 3.2–3.4)
- [ ] No coil whine / buzzing from PSU/VRM

### Environmental Validation
- [ ] Ambient 18–24°C at idle
- [ ] Ambient ≤ 28°C at full load (4h stress)
- [ ] Humidity 30–60% RH
- [ ] Zero condensation after cold→warm cycle test

### Safety Validation
- [ ] Smoke detector tested (test button)
- [ ] Extinguisher accessible, charged
- [ ] All cables secured, no trip hazards
- [ ] Emergency shutdown procedure documented & posted

---

## 11. Bill of Materials (Safety & Cooling)

| Item | Qty (3 nodes) | Spec | Est. Cost |
|------|---------------|------|-----------|
| CPU Tower Cooler (4-heatpipe, 120mm) | 3 | ID-COOLING SE-214-XT / Deepcool AK620 | ~3×1500₽ |
| Case Fans 120mm PWM | 9–12 | Arctic P12 / P14 / be quiet! Pure Wings 2 | ~12×400₽ |
| Dust Filters (magnetic/velcro) | 6–9 | 120mm/140mm magnetic mesh | ~9×200₽ |
| Thermal Paste (4g) | 1 | NT-H2 / MX-6 / GD900 | ~500₽ |
| Surge Protector (2000J+) | 3 | APC / Legrand / IEK | ~3×800₽ |
| UPS 600-800VA | 3 (optional) | APC Back-UPS / Eaton 5E | ~3×8000₽ |
| Space Heater + Thermostat | 1 | Ceramic 1000W + digital thermostat | ~2500₽ |
| Temp/Humidity Sensors | 4 | Aqara / Sonoff Zigbee | ~4×800₽ |
| Conformal Coating | 1 can | MG Chemicals 422B / Electrolube DCA | ~1500₽ |
| Anti-static Kit | 1 | Mat + strap + bags | ~1000₽ |
| CO₂ Extinguisher 2kg | 1 | Certified | ~3000₽ |
| **Total (without UPS)** | | | **~18,000₽** |
| **Total (with UPS)** | | | **~42,000₽** |

---

## 12. Compliance & Standards Reference

| Standard | Applies To |
|----------|------------|
| **IEC 60950-1 / 62368-1** | PSU, equipment safety |
| **IEC 61000-4-2/4/5** | ESD, surge, EFT immunity |
| **UL 94 V-0** | Cable flame rating |
| **ASHRAE TC 9.9** | Data center thermal guidelines (adapted for home) |
| **GOST R 51317** | Russian electrical safety (if applicable) |

---

## 13. Quick Reference Card (Post on Rack)

```
╔══════════════════════════════════════════════════════════╗
║           ZEER BIG DATA — EMERGENCY QUICK REF            ║
╠══════════════════════════════════════════════════════════╣
║  OVERHEAT (>85°C):  Auto-throttle → graceful shutdown    ║
║  OVERHEAT (>92°C):  IMMEDIATE POWEROFF                   ║
║  AMBIENT <10°C:     DO NOT BOOT — warm room first        ║
║  WATER LEAK:        KILL POWER → dry 24h → inspect       ║
║  SMOKE/FIRE:        CO₂ EXTINGUISHER → POWER OFF → 112   ║
║                                                         ║
║  MONITORING: http://zeer-master:3001 (Grafana)           ║
║  ALERTS:     Telegram @zeer_bigdata_alerts               ║
║  LOGS:       /opt/zeer/logs/thermal-watchdog.log         ║
║                                                         ║
║  NORMAL CPU:  45–65°C    MAX SAFE: 80°C                  ║
║  NORMAL DISK: 30–45°C    MAX SAFE: 55°C                  ║
║  AMBIENT:     18–24°C    MIN BOOT: 12°C                  ║
╚══════════════════════════════════════════════════════════╝
```

---

## Appendix A: BIOS Settings Checklist (Per Node)

```
Advanced → CPU Configuration:
  [x] Intel Turbo Boost: ENABLED
  [x] CPU C-States: ENABLED (C6/C7 for idle cooling)
  [x] Enhanced Intel SpeedStep: ENABLED
  [x] Hyper-Threading: ENABLED

Advanced → Thermal:
  [x] CPU Fan Control: PWM / Manual Curve
  [x] Critical Temp: 90°C (Sandy) / 95°C (Ivy)
  [x] Shutdown Temp: 95°C / 100°C

Advanced → Power:
  [x] Restore AC Power Loss: LAST STATE (or POWER ON)
  [x] Deep Sleep: DISABLED (for 24/7)

Boot:
  [x] Fast Boot: DISABLED (allows memory training)
  [x] NumLock: ON
```

---

## Appendix B: `sensors` Output Reference (Healthy Idle)

```text
coretemp-isa-0000
Adapter: ISA adapter
Package id 0:  +38.0°C  (high = +80.0°C, crit = +95.0°C)
Core 0:        +35.0°C  (high = +80.0°C, crit = +95.0°C)
Core 1:        +36.0°C  (high = +80.0°C, crit = +95.0°C)
Core 2:        +37.0°C  (high = +80.0°C, crit = +95.0°C)
Core 3:        +34.0°C  (high = +80.0°C, crit = +95.0°C)

nvme-pci-0100   (if NVMe adapter used)
Adapter: PCI adapter
Composite:     +32.0°C  (low  = -20.0°C, high = +80.0°C)
```

---

**Document Owner:** Infrastructure Team  
**Review Cycle:** Quarterly or after any thermal incident  
**Next Review:** 2026-12-22