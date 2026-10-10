"""
REST API and web dashboard for the energy-aware SDN controller.

``NetworkMonitoringAPI`` registers ``NetworkAPIController`` on ryu's WSGI service
(``--wsapi-host`` / ``--wsapi-port``, port 8080 by default). The handlers read the other
Ryu apps directly: the apps are resolved by their brick names (``TopologyManagerV2``,
``NetworkMonitor``, ``detector``, ``Forwarding``), re-resolved on every switch state
change, and every 10 s until all four are present. Everything is read-only except
``POST /api/v1/agents/agent1/enable``.

Endpoints (all under ``/api/v1``):
  topology, topology/paths/{src}/{dst}, topology/hosts, topology/details
  stats/utilization, stats/paths, stats/paths/{path_id}
  flows/tracked, flows/rewards
  delay/stats, delay/link/{src}/{dst}
  agents/status, agents/agent1/enable (POST), agents/agent1/probabilities
  mptcp/flows, system/overview
  energy/network, energy/paths, energy/rewards, energy/bw-limits, energy/meters, energy/flows
Pages: /dashboard, /topology-view, /flow-monitoring, /agent-control, served together with
their static files from the ``web/`` directory next to this module.
"""

from ryu.app.wsgi import ControllerBase, WSGIApplication, route
from ryu.base import app_manager
from ryu.base.app_manager import lookup_service_brick
from ryu.controller import ofp_event
from ryu.controller.handler import set_ev_cls, MAIN_DISPATCHER, DEAD_DISPATCHER
from ryu.lib import hub
from webob import Response
import json
import logging
import os
from datetime import datetime

import setting

LOG = logging.getLogger('ryu.app.rest_api_v2')

# /api/v1/energy/paths: default and hard ceiling on the number of rows returned
ENERGY_PATHS_DEFAULT_LIMIT = 200
ENERGY_PATHS_MAX_LIMIT = 6120

# Templates and static files live next to this module.
WEB_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'web')


class NetworkMonitoringAPI(app_manager.RyuApp):
    """Registers the REST controller and holds the references to the other apps."""
    _CONTEXTS = {
        'wsgi': WSGIApplication
    }

    def __init__(self, *args, **kwargs):
        super(NetworkMonitoringAPI, self).__init__(*args, **kwargs)
        self.name = 'NetworkMonitoringAPI'

        wsgi = kwargs['wsgi']
        wsgi.register(NetworkAPIController, {'api_app': self})

        # Resolved lazily: the other apps may not have started yet.
        self.topology_manager = None
        self.network_monitor = None
        self.delay_detector = None
        self.forwarding = None
        self._last_ref_status = None   # last logged (tm, nm, dd, fw) presence tuple

        LOG.info("Network Monitoring API initialized")
        self.update_thread = hub.spawn(self._periodic_update)

    def _required_refs_ready(self):
        return all(x is not None for x in (self.topology_manager, self.network_monitor,
                                           self.delay_detector, self.forwarding))

    def get_module_references(self):
        """Resolve the other Ryu apps. Cheap (dict lookups), so it may run on every switch
        state change; the status is logged only when it changes."""
        try:
            self.topology_manager = lookup_service_brick('TopologyManagerV2')
            self.network_monitor = lookup_service_brick('NetworkMonitor')
            self.delay_detector = lookup_service_brick('detector')
            self.forwarding = lookup_service_brick('Forwarding')

            status = (self.topology_manager is not None, self.network_monitor is not None,
                      self.delay_detector is not None, self.forwarding is not None)
            if status != self._last_ref_status:
                self._last_ref_status = status
                LOG.info("Module references updated: TopologyManager=%s NetworkMonitor=%s "
                         "DelayDetector=%s Forwarding=%s", *status)
        except Exception as e:
            LOG.error(f"Error getting module references: {e}")

    def _periodic_update(self):
        """Re-resolve every 10 s until all four apps are present, then idle."""
        while True:
            hub.sleep(10)
            if not self._required_refs_ready():
                self.get_module_references()

    @set_ev_cls(ofp_event.EventOFPStateChange, [MAIN_DISPATCHER, DEAD_DISPATCHER])
    def state_change_handler(self, ev):
        """Re-resolve the module references on every switch join / leave."""
        self.get_module_references()

    def close(self):
        """ryu calls close() on shutdown; stop the re-resolve greenlet."""
        try:
            hub.kill(self.update_thread)
        except Exception as e:
            LOG.error(f"Error stopping the update thread: {e}")
        super(NetworkMonitoringAPI, self).close()


class NetworkAPIController(ControllerBase):
    """WSGI controller with the JSON endpoints and the dashboard pages."""

    def __init__(self, req, link, data, **config):
        super(NetworkAPIController, self).__init__(req, link, data, **config)
        self.api_app = data['api_app']

    # ==================== Helper Methods ====================

    def _get_modules(self):
        """(topology_manager, network_monitor, delay_detector, forwarding); any may be None."""
        if not self.api_app.topology_manager:
            self.api_app.get_module_references()
        return (
            self.api_app.topology_manager,
            self.api_app.network_monitor,
            self.api_app.delay_detector,
            self.api_app.forwarding
        )

    def _json_response(self, data, status=200):
        """Create JSON response"""
        body = json.dumps(data, indent=2)
        return Response(
            content_type='application/json',
            charset='UTF-8',
            body=body,
            status=status
        )

    def _error_response(self, message, status=500):
        """Create error response"""
        return self._json_response({'error': message}, status)

    def _html_response(self, html_content):
        """Create HTML response"""
        return Response(
            content_type='text/html',
            charset='UTF-8',
            body=html_content
        )

    # ==================== Topology Endpoints ====================

    @route('topology', '/api/v1/topology', methods=['GET'])
    def get_topology(self, req, **kwargs):
        """Topology summary: switch / link / host / path counts and the topology version"""
        try:
            topology_mgr, _, _, _ = self._get_modules()
            if not topology_mgr:
                return self._error_response("Topology Manager not available", 503)

            return self._json_response(topology_mgr.get_topology_summary())
        except Exception as e:
            return self._error_response(str(e))

    @route('topology', '/api/v1/topology/paths/{src}/{dst}', methods=['GET'])
    def get_paths(self, req, src, dst, **kwargs):
        """The K candidate paths between two switches"""
        try:
            topology_mgr, _, _, _ = self._get_modules()
            if not topology_mgr:
                return self._error_response("Topology Manager not available", 503)

            src_dpid = int(src)
            dst_dpid = int(dst)
            paths = topology_mgr.get_path_info(src_dpid, dst_dpid)

            return self._json_response({
                'source': src_dpid,
                'destination': dst_dpid,
                'paths': paths
            })
        except ValueError:
            return self._error_response("Invalid DPID format", 400)
        except Exception as e:
            return self._error_response(str(e))

    @route('topology', '/api/v1/topology/hosts', methods=['GET'])
    def get_hosts(self, req, **kwargs):
        """Known hosts with their access switch and port"""
        try:
            topology_mgr, _, _, _ = self._get_modules()
            if not topology_mgr:
                return self._error_response("Topology Manager not available", 503)

            hosts = {}
            for ip, (dpid, port) in topology_mgr.hosts.items():
                hosts[ip] = {
                    'ip': ip,
                    'switch': dpid,
                    'port': port
                }

            return self._json_response({'hosts': hosts})
        except Exception as e:
            return self._error_response(str(e))

    @route('topology', '/api/v1/topology/details', methods=['GET'])
    def get_topology_details(self, req, **kwargs):
        """Topology for visualization: {nodes: [{id, type, ...}], links: [{from, to, type, ...}]}"""
        try:
            topology_mgr, _, _, _ = self._get_modules()
            if not topology_mgr:
                return self._error_response("Topology Manager not available", 503)

            return self._json_response(topology_mgr.get_topology_details())
        except Exception as e:
            return self._error_response(str(e))

    # ==================== Network Statistics Endpoints ====================

    @route('stats', '/api/v1/stats/utilization', methods=['GET'])
    def get_utilization(self, req, **kwargs):
        """Bounded utilization (0..UTILIZATION_MAX) of the last monitor cycle:
        link_utilization {"s<src>:s<dst>": u} and port_utilization {"<dpid>:<port>": u}"""
        try:
            _, network_mon, _, _ = self._get_modules()
            if not network_mon:
                return self._error_response("Network Monitor not available", 503)

            return self._json_response({
                'link_utilization': dict(network_mon.link_utilization_links),
                'port_utilization': dict(network_mon.link_utilization_ports),
                'timestamp': datetime.now().isoformat()
            })
        except Exception as e:
            return self._error_response(str(e))

    @route('stats', '/api/v1/stats/paths', methods=['GET'])
    def get_path_stats(self, req, **kwargs):
        """Agent1 state per path: {path_stats: {"<src>-<dst>-pathN": [p_norm, alu, mlu, delay_rank]}}"""
        try:
            _, network_mon, _, _ = self._get_modules()
            if not network_mon:
                return self._error_response("Network Monitor not available", 503)

            path_stats = dict(network_mon.get_path_stats_all())

            return self._json_response({
                'path_stats': path_stats,
                'count': len(path_stats)
            })
        except Exception as e:
            return self._error_response(str(e))

    @route('stats', '/api/v1/stats/paths/{path_id}', methods=['GET'])
    def get_path_stat(self, req, path_id, **kwargs):
        """The [p_norm, alu, mlu, delay_rank] state of one path; 404 for an unknown path id"""
        try:
            _, network_mon, _, _ = self._get_modules()
            if not network_mon:
                return self._error_response("Network Monitor not available", 503)

            stats = network_mon.get_path_stats(path_id)
            if stats is None:
                return self._error_response(f"Path {path_id} not found", 404)

            return self._json_response({
                'path_id': path_id,
                'stats': stats
            })
        except Exception as e:
            return self._error_response(str(e))

    # ==================== Flow Endpoints ====================

    @route('flows', '/api/v1/flows/tracked', methods=['GET'])
    def get_tracked_flows(self, req, **kwargs):
        """Live flow-ledger counts: {tracked: {total, tcp_fwd, tcp_rev, mptcp, other, paths_with_fwd_flows}}"""
        try:
            _, _, _, forwarding = self._get_modules()
            if not forwarding:
                return self._error_response("Forwarding module not available", 503)

            counts = dict(forwarding.get_ledger_counts() or {})
            return self._json_response({
                'source': 'ledger',
                'tracked': counts,
                'count': counts.get('total', 0)
            })
        except Exception as e:
            return self._error_response(str(e))

    @route('flows', '/api/v1/flows/rewards', methods=['GET'])
    def get_path_rewards(self, req, **kwargs):
        """Agent1's rewards of the last cycle: {path_rewards: {path_key: reward}}"""
        try:
            _, _, _, forwarding = self._get_modules()
            if not forwarding:
                return self._error_response("Forwarding module not available", 503)

            rewards = dict(getattr(forwarding, 'agent1_reward', None) or {})
            return self._json_response({
                'source': 'agent1',
                'path_rewards': rewards,
                'count': len(rewards)
            })
        except Exception as e:
            return self._error_response(str(e))

    # ==================== Delay Endpoints ====================

    @route('delay', '/api/v1/delay/stats', methods=['GET'])
    def get_delay_stats(self, req, **kwargs):
        """Controller-to-switch echo delays and the per-link delays derived from LLDP"""
        try:
            _, _, delay_det, _ = self._get_modules()
            if not delay_det:
                return self._error_response("Delay Detector not available", 503)

            echo_delays = dict(delay_det.echo_delay_table)
            lldp_delays = {src: dict(dsts) for src, dsts in delay_det.lldp_delay_table.items()}

            # A link's delay needs both LLDP directions and both echo samples; skip the
            # links that are not fully measured yet.
            link_delays = {}
            for src in lldp_delays:
                for dst in lldp_delays[src]:
                    try:
                        delay = delay_det.calculate_delay(src, dst)
                    except KeyError:
                        continue
                    link_delays[f"{src}-{dst}"] = {
                        'delay_ms': delay * 1000,
                        'raw_delay': lldp_delays[src][dst]
                    }

            return self._json_response({
                'echo_delays': echo_delays,
                'link_delays': link_delays,
                'timestamp': datetime.now().isoformat()
            })
        except Exception as e:
            return self._error_response(str(e))

    @route('delay', '/api/v1/delay/link/{src}/{dst}', methods=['GET'])
    def get_link_delay(self, req, src, dst, **kwargs):
        """Raw LLDP delay sample of one directed link"""
        try:
            _, _, delay_det, _ = self._get_modules()
            if not delay_det:
                return self._error_response("Delay Detector not available", 503)

            src, dst = int(src), int(dst)
            try:
                delay = delay_det.get_delay_stats(src, dst)
            except KeyError:
                return self._error_response(f"No delay sample for link {src}-{dst}", 404)

            return self._json_response({
                'source': src,
                'destination': dst,
                'delay_seconds': delay,
                'delay_ms': delay * 1000
            })
        except ValueError:
            return self._error_response("Invalid DPID format", 400)
        except Exception as e:
            return self._error_response(str(e))

    # ==================== Agent Control Endpoints ====================

    @route('agents', '/api/v1/agents/status', methods=['GET'])
    def get_agents_status(self, req, **kwargs):
        """Agent1 status: enabled, ready, device, table sizes"""
        try:
            _, _, _, forwarding = self._get_modules()
            if not forwarding:
                return self._error_response("Forwarding module not available", 503)

            agent1_status = {
                'enabled': forwarding.use_agent1,
                'ready': forwarding.agent1_ready,
                'device': forwarding.check_agent_device(),
                'path_probabilities': len(forwarding.agent1_path_probabilities),
                'rewards_collected': len(forwarding.agent1_reward)
            }

            return self._json_response({
                'agent1': agent1_status,
                'timestamp': datetime.now().isoformat()
            })
        except Exception as e:
            return self._error_response(str(e))

    @route('agents', '/api/v1/agents/agent1/enable', methods=['POST'])
    def enable_agent1(self, req, **kwargs):
        """Enable / disable Agent1 path selection. Body: {"enabled": true|false}"""
        try:
            _, _, _, forwarding = self._get_modules()
            if not forwarding:
                return self._error_response("Forwarding module not available", 503)

            data = json.loads(req.body)
            enabled = bool(data.get('enabled', True))

            forwarding.use_agent1 = enabled

            return self._json_response({
                'status': 'success',
                'agent1_enabled': enabled
            })
        except ValueError:
            return self._error_response("Invalid JSON body", 400)
        except Exception as e:
            return self._error_response(str(e))

    @route('agents', '/api/v1/agents/agent1/probabilities', methods=['GET'])
    def get_agent1_probabilities(self, req, **kwargs):
        """Agent1's path probabilities of the last cycle"""
        try:
            _, _, _, forwarding = self._get_modules()
            if not forwarding:
                return self._error_response("Forwarding module not available", 503)

            return self._json_response({
                'path_probabilities': dict(forwarding.agent1_path_probabilities),
                'path_db_probability': dict(forwarding.Path_db_probability),
                'timestamp': forwarding.agent1_timestamp
            })
        except Exception as e:
            return self._error_response(str(e))

    # ==================== MPTCP Endpoints ====================

    @route('mptcp', '/api/v1/mptcp/flows', methods=['GET'])
    def get_mptcp_flows(self, req, **kwargs):
        """Tracked MPTCP connections (by receiver token) and their subflows"""
        try:
            _, _, _, forwarding = self._get_modules()
            if not forwarding:
                return self._error_response("Forwarding module not available", 503)

            mptcp_flows = []
            for token, subflows in list(forwarding.mptcp_flows.items()):
                flow_info = {
                    'receiver_token': token,
                    'subflows': {}
                }
                for subflow_id, subflow_data in list(subflows.items()):
                    flow_info['subflows'][subflow_id] = {
                        'src_ip': subflow_data.get('src_ip'),
                        'dst_ip': subflow_data.get('dst_ip'),
                        'path_key': subflow_data.get('path_key'),
                        'cookie': subflow_data.get('cookie')
                    }
                mptcp_flows.append(flow_info)

            return self._json_response({
                'mptcp_flows': mptcp_flows,
                'count': len(mptcp_flows)
            })
        except Exception as e:
            return self._error_response(str(e))

    # ==================== System Overview Endpoint ====================

    @route('system', '/api/v1/system/overview', methods=['GET'])
    def get_system_overview(self, req, **kwargs):
        """Module presence, topology summary, monitor counts, Agent1 state, meters and the energy row"""
        try:
            topology_mgr, network_mon, delay_det, forwarding = self._get_modules()

            overview = {
                'timestamp': datetime.now().isoformat(),
                'modules': {
                    'topology_manager': topology_mgr is not None,
                    'network_monitor': network_mon is not None,
                    'delay_detector': delay_det is not None,
                    'forwarding': forwarding is not None
                }
            }

            if topology_mgr:
                overview['topology'] = topology_mgr.get_topology_summary()

            if network_mon:
                overview['network_stats'] = {
                    'monitored_switches': len(network_mon.datapaths_table),
                    'path_stats_count': len(network_mon.Agent1_PathStats)
                }

            if forwarding:
                overview['agents'] = {
                    'agent1_enabled': forwarding.use_agent1,
                    'mptcp_flows': len(forwarding.mptcp_flows)
                }
                meter_mgr = getattr(forwarding, 'meter_manager', None)
                overview['meters_active'] = meter_mgr.count() if meter_mgr is not None else None

            # Energy row of the latest monitor cycle (None before the first cycle / no monitor)
            overview['energy'] = self._network_energy_row(network_mon)

            return self._json_response(overview)

        except Exception as e:
            return self._error_response(str(e))

    # ==================== Energy / BW-limit Endpoints (read-only) ====================
    #
    # Everything below only READS forwarding / network_monitor state that is rebound
    # (never mutated) by its single writer greenlet, so a dict(...) copy taken here is a
    # consistent view. Each endpoint answers 503 when the Forwarding app is missing and
    # otherwise reads its attributes.

    @staticmethod
    def _query(req, name, default=None):
        """Query-string parameter (webob Request.GET); an absent or empty value gives default."""
        value = req.GET.get(name)
        return default if value in (None, '') else value

    @staticmethod
    def _network_energy_row(network_mon):
        """network_mon.get_network_stats() as a plain dict copy, or None."""
        if network_mon is None or not hasattr(network_mon, 'get_network_stats'):
            return None
        row = network_mon.get_network_stats()
        return dict(row) if row else None

    @staticmethod
    def _energy_settings():
        """The setting.py knobs that shape the energy reward and the BW limits."""
        names = ('POWER_CONSUMPTION_FACTOR', 'AGENT1_REWARD_PATH_WEIGHT',
                 'AGENT1_PROP_POWER_MODE', 'AGENT1_PATH_TERM_SCALE', 'AGENT1_REWARD_ALL_PATHS',
                 'ENABLE_BW_LIMIT', 'BW_LIMIT_DRY_RUN', 'BW_MIN_Mbps', 'BW_MAX_Mbps',
                 'BW_PROB_NORMALIZATION')
        return {n: getattr(setting, n) for n in names}

    @staticmethod
    def _tuple_key_counts(counts):
        """{(a, b): n} -> {"a:b": n} so json.dumps accepts the keys."""
        out = {}
        for key, n in dict(counts or {}).items():
            if isinstance(key, (tuple, list)):
                skey = ':'.join(str(k) for k in key)
            else:
                skey = str(key)
            out[skey] = n
        return out

    @route('energy', '/api/v1/energy/network', methods=['GET'])
    def get_energy_network(self, req, **kwargs):
        """Network-wide energy row of the latest monitor cycle + the settings that shape it"""
        try:
            _, network_mon, _, _ = self._get_modules()
            if not network_mon:
                return self._error_response("Network Monitor not available", 503)

            row = self._network_energy_row(network_mon)
            if row is None:
                return self._error_response("no energy snapshot yet", 503)
            row['settings'] = self._energy_settings()
            return self._json_response(row)
        except Exception as e:
            return self._error_response(str(e))

    @route('energy', '/api/v1/energy/paths', methods=['GET'])
    def get_energy_paths(self, req, **kwargs):
        """Per-path energy view: power/ALU/MLU, probability, BW limit, n_active, last reward.
        Query: ?prefix=201-202 (path_key prefix filter) &limit=200 (max 6120)"""
        try:
            _, network_mon, _, forwarding = self._get_modules()
            if not network_mon or not hasattr(network_mon, 'get_energy_snapshot'):
                return self._error_response("Network Monitor not available", 503)

            snap = network_mon.get_energy_snapshot()
            if snap is None:
                return self._error_response("no energy snapshot yet", 503)

            prefix = self._query(req, 'prefix')
            limit_raw = self._query(req, 'limit', ENERGY_PATHS_DEFAULT_LIMIT)
            try:
                limit = int(limit_raw)
            except (TypeError, ValueError):
                return self._error_response("Invalid limit (must be an integer)", 400)
            limit = max(1, min(limit, ENERGY_PATHS_MAX_LIMIT))

            # Consistent copies of the (rebound-only) forwarding tables.
            paths = dict(snap.paths)
            states = dict(snap.states)
            if forwarding is not None:
                prob_db = dict(getattr(forwarding, 'Path_db_probability', None) or {})
                bw_limits = dict(getattr(forwarding, 'path_bw_limit', None) or {})
                rewards = dict(getattr(forwarding, 'agent1_reward', None) or {})
                terms = dict(getattr(forwarding, 'agent1_reward_terms', None) or {})
                get_active = getattr(forwarding, 'get_active_flow_count_by_path', None)
                n_active = dict(get_active() or {}) if callable(get_active) else {}
            else:
                prob_db, bw_limits, rewards, terms, n_active = {}, {}, {}, {}, {}

            rows = []
            for key in sorted(paths, key=str):
                if prefix and not str(key).startswith(prefix):
                    continue
                if len(rows) >= limit:
                    break
                pe = paths[key]
                state = states.get(key)
                prob_entry = prob_db.get(key)
                probability = prob_entry.get('probability') if isinstance(prob_entry, dict) else None
                term = terms.get(key)
                rows.append({
                    'path_key': key,
                    'p_path': getattr(pe, 'p_path', None),
                    'alu': getattr(pe, 'alu', None),
                    'mlu': getattr(pe, 'mlu', None),
                    'n_hops': getattr(pe, 'n_hops', None),
                    'delay_rank': getattr(pe, 'delay_rank', None),
                    'p_norm': state[0] if state else None,
                    'probability': probability,
                    'bw_limit_mbps': bw_limits.get(key),
                    'n_active': n_active.get(key, 0),
                    'last_reward': rewards.get(key),
                    'terms': dict(term) if isinstance(term, dict) else term,
                })

            return self._json_response({
                'cycle': snap.cycle,
                'ts': snap.ts,
                'count': len(rows),
                'paths': rows
            })
        except Exception as e:
            return self._error_response(str(e))

    @route('energy', '/api/v1/energy/rewards', methods=['GET'])
    def get_energy_rewards(self, req, **kwargs):
        """Agent1's last per-path rewards and their term breakdown"""
        try:
            _, _, _, forwarding = self._get_modules()
            if not forwarding:
                return self._error_response("Forwarding module not available", 503)

            rewards = dict(getattr(forwarding, 'agent1_reward', None) or {})
            terms = {k: (dict(v) if isinstance(v, dict) else v)
                     for k, v in dict(getattr(forwarding, 'agent1_reward_terms', None) or {}).items()}
            values = [v for v in rewards.values() if isinstance(v, (int, float))]
            return self._json_response({
                'cycle': getattr(forwarding, '_last_energy_cycle', None),
                'agent1_cycle': getattr(forwarding, 'agent1_cycle_count', None),
                'count': len(rewards),
                'mean': (sum(values) / len(values)) if values else None,
                'min': min(values) if values else None,
                'max': max(values) if values else None,
                'rewards': rewards,
                'terms': terms
            })
        except Exception as e:
            return self._error_response(str(e))

    @route('energy', '/api/v1/energy/bw-limits', methods=['GET'])
    def get_energy_bw_limits(self, req, **kwargs):
        """Per-path BW limits (Mbps) of the last Agent1 cycle.
        Query: ?prefix=201-202 &active_only=1 (only paths with live forward flows)"""
        try:
            _, _, _, forwarding = self._get_modules()
            if not forwarding:
                return self._error_response("Forwarding module not available", 503)

            prefix = self._query(req, 'prefix')
            active_only = str(self._query(req, 'active_only', '0')).lower() in ('1', 'true', 'yes')

            limits = dict(getattr(forwarding, 'path_bw_limit', None) or {})
            if prefix:
                limits = {k: v for k, v in limits.items() if str(k).startswith(prefix)}
            if active_only:
                get_active = getattr(forwarding, 'get_active_flow_count_by_path', None)
                active = dict(get_active() or {}) if callable(get_active) else {}
                limits = {k: v for k, v in limits.items() if k in active}

            bw_cfg = getattr(forwarding, 'bw_cfg', None)
            config = bw_cfg._asdict() if hasattr(bw_cfg, '_asdict') else None
            return self._json_response({
                'enabled': setting.ENABLE_BW_LIMIT,
                'dry_run': setting.BW_LIMIT_DRY_RUN,
                'stats': dict(getattr(forwarding, 'bw_limit_stats', None) or {}),
                'config': dict(config) if config else None,
                'count': len(limits),
                'limits': limits
            })
        except Exception as e:
            return self._error_response(str(e))

    @route('energy', '/api/v1/energy/meters', methods=['GET'])
    def get_energy_meters(self, req, **kwargs):
        """MeterManager snapshot, flow lifecycle + OpenFlow error counters, worker-pool counters"""
        try:
            _, _, _, forwarding = self._get_modules()
            if not forwarding:
                return self._error_response("Forwarding module not available", 503)

            meter_mgr = getattr(forwarding, 'meter_manager', None)
            lifecycle = {}
            for k, v in dict(getattr(forwarding, 'lifecycle_stats', None) or {}).items():
                lifecycle[k] = dict(v) if isinstance(v, dict) else v

            pool = getattr(forwarding, 'worker_pool', None)
            pool_stats_fn = getattr(pool, 'pool_stats', None)
            pool_stats = pool_stats_fn() if callable(pool_stats_fn) else None
            counters = pool_stats.get('counters') if isinstance(pool_stats, dict) else None
            dropped = counters.get('agent_reports_dropped') if isinstance(counters, dict) else None

            return self._json_response({
                'meter_manager': meter_mgr.snapshot() if meter_mgr is not None else None,
                'lifecycle': lifecycle,
                'of_errors': self._tuple_key_counts(getattr(forwarding, 'of_error_counts', None)),
                'agent_reports_dropped': dropped,
                'pool_stats': pool_stats,
            })
        except Exception as e:
            return self._error_response(str(e))

    @route('energy', '/api/v1/energy/flows', methods=['GET'])
    def get_energy_flows(self, req, **kwargs):
        """Flow-ledger counts by kind/direction and path-activity coverage"""
        try:
            _, _, _, forwarding = self._get_modules()
            if not forwarding:
                return self._error_response("Forwarding module not available", 503)

            get_counts = getattr(forwarding, 'get_ledger_counts', None)
            get_activity = getattr(forwarding, 'get_paths_with_activity', None)
            get_active = getattr(forwarding, 'get_active_flow_count_by_path', None)
            window = setting.AGENT1_ACTIVITY_WINDOW_S
            return self._json_response({
                'ledger': dict(get_counts() or {}) if callable(get_counts) else None,
                'activity_window_s': window,
                'paths_with_activity': len(get_activity(window) or ()) if callable(get_activity) else None,
                'n_active_by_path_count': len(get_active() or {}) if callable(get_active) else None
            })
        except Exception as e:
            return self._error_response(str(e))

    # ==================== Static File Serving ====================

    def _serve_static(self, subdir, filename, content_type):
        """Serve one file from web/static/<subdir>/."""
        path = os.path.join(WEB_ROOT, 'static', subdir, filename)
        if not os.path.isfile(path):
            return self._error_response(f"{subdir.upper()} file not found", 404)
        try:
            with open(path, 'r') as f:
                content = f.read()
            return Response(content_type=content_type, charset='UTF-8', body=content)
        except Exception as e:
            return self._error_response(f"Error reading {subdir.upper()} file: {str(e)}", 500)

    @route('static_css', '/web/static/css/{filename}', methods=['GET'])
    def serve_css(self, req, filename, **kwargs):
        """Serve CSS files"""
        return self._serve_static('css', filename, 'text/css')

    @route('static_js', '/web/static/js/{filename}', methods=['GET'])
    def serve_js(self, req, filename, **kwargs):
        """Serve JavaScript files"""
        return self._serve_static('js', filename, 'application/javascript')

    def _load_template(self, template_name):
        """Load an HTML template from web/templates/."""
        template_path = os.path.join(WEB_ROOT, 'templates', template_name)

        if not os.path.exists(template_path):
            raise FileNotFoundError(f"Template {template_name} not found")

        with open(template_path, 'r') as f:
            return f.read()

    # ==================== Web Interface Endpoints ====================

    @route('dashboard', '/dashboard', methods=['GET'])
    def dashboard(self, req, **kwargs):
        """Main dashboard with network overview"""
        try:
            html = self._load_template('dashboard.html')
            return self._html_response(html)
        except FileNotFoundError:
            return self._error_response("Dashboard template not found", 404)
        except Exception as e:
            return self._error_response(f"Error loading dashboard: {str(e)}", 500)

    @route('topology_view', '/topology-view', methods=['GET'])
    def topology_view(self, req, **kwargs):
        """Network topology visualization page"""
        try:
            html = self._load_template('topology.html')
            return self._html_response(html)
        except FileNotFoundError:
            return self._error_response("Topology template not found", 404)
        except Exception as e:
            return self._error_response(f"Error loading topology view: {str(e)}", 500)

    @route('flow_monitoring', '/flow-monitoring', methods=['GET'])
    def flow_monitoring(self, req, **kwargs):
        """Flow monitoring page"""
        try:
            html = self._load_template('flow-monitoring.html')
            return self._html_response(html)
        except FileNotFoundError:
            return self._error_response("Flow monitoring template not found", 404)
        except Exception as e:
            return self._error_response(f"Error loading flow monitoring: {str(e)}", 500)

    @route('agent_control', '/agent-control', methods=['GET'])
    def agent_control(self, req, **kwargs):
        """Agent control page"""
        try:
            html = self._load_template('agent-control.html')
            return self._html_response(html)
        except FileNotFoundError:
            return self._error_response("Agent control template not found", 404)
        except Exception as e:
            return self._error_response(f"Error loading agent control: {str(e)}", 500)
