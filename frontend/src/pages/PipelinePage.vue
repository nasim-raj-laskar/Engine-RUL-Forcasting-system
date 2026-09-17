<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import DashboardLayout from '../layouts/DashboardLayout.vue'
import { useEngineStore } from '../stores/engineStore'
import { getHealth } from '../services/api'

const store = useEngineStore()

// ── Service health checks ─────────────────────────────────────────────────────
interface ServiceStatus {
  name: string
  port: number
  url: string
  color: string
  status: 'up' | 'down' | 'checking'
  icon: string
}

const services = ref<ServiceStatus[]>([
  { name: 'FastAPI Inference',   port: 8000, url: 'http://localhost:8000/health',              color: 'text-cyan-400',   status: 'checking', icon: '🧠' },
  { name: 'Redis Feature Store', port: 6379, url: '',                                          color: 'text-green-400',  status: 'checking', icon: '💾' },
  { name: 'Solace PubSub+',     port: 8080, url: 'http://localhost:8080/SEMP/v2/config',      color: 'text-violet-400', status: 'checking', icon: '📡' },
  { name: 'Kafka Broker',       port: 9092, url: 'http://localhost:8082/overview',             color: 'text-amber-400',  status: 'checking', icon: '📨' },
  { name: 'Kafka Connect',      port: 8083, url: 'http://localhost:8083/connectors',           color: 'text-orange-400', status: 'checking', icon: '🔌' },
  { name: 'Flink JobManager',   port: 8082, url: 'http://localhost:8082/overview',             color: 'text-sky-400',    status: 'checking', icon: '⚡' },
  { name: 'Prometheus',         port: 9090, url: 'http://localhost:9090/-/healthy',            color: 'text-purple-400', status: 'checking', icon: '📊' },
  { name: 'Grafana',            port: 3000, url: 'http://localhost:3000/api/health',           color: 'text-pink-400',   status: 'checking', icon: '📈' },
])

// ── Flink job status ──────────────────────────────────────────────────────────
interface FlinkJob { id: string; status: string }
const flinkJobs = ref<FlinkJob[]>([])

async function fetchFlinkJobs() {
  try {
    const res = await fetch('http://localhost:8082/jobs', { signal: AbortSignal.timeout(3000) })
    const data = await res.json()
    flinkJobs.value = (data.jobs || []).filter((j: FlinkJob) => j.status === 'RUNNING')
  } catch { flinkJobs.value = [] }
}

async function checkServices() {
  try { await getHealth(); services.value[0].status = 'up' } catch { services.value[0].status = 'down' }
  services.value[1].status = store.wsConnected ? 'up' : 'down'
  for (const svc of services.value.slice(2)) {
    if (!svc.url) continue
    try {
      await fetch(svc.url, { mode: 'no-cors', signal: AbortSignal.timeout(2000) })
      svc.status = 'up'
    } catch { svc.status = 'down' }
  }
  await fetchFlinkJobs()
}

onMounted(() => { checkServices(); setInterval(checkServices, 30_000) })

// ── Live counters from WS streams ─────────────────────────────────────────────
const activeEngines  = computed(() => store.predictions.size)
const telemetryCount = computed(() => store.telemetry.size)
const criticalCount  = computed(() => store.criticalCount)
const avgCycle = computed(() => {
  const metas = [...store.telemetry.values()]
  if (!metas.length) return 0
  return Math.round(metas.reduce((s, m) => s + m.cycle, 0) / metas.length)
})

// ── Pipeline topology (8 stages) ─────────────────────────────────────────────
const pipeline = [
  { label: 'Telemetry Producer',  desc: '100 engines · risk-distributed',     icon: '📡', gradient: 'from-cyan-500/20 to-cyan-600/5',   border: 'border-cyan-500/40',   dot: 'bg-cyan-400' },
  { label: 'Solace PubSub+',     desc: 'SMF ingestion broker',                icon: '🔗', gradient: 'from-violet-500/20 to-violet-600/5', border: 'border-violet-500/40', dot: 'bg-violet-400' },
  { label: 'Kafka Connector',    desc: 'Solace → Kafka bridge',               icon: '🔌', gradient: 'from-orange-500/20 to-orange-600/5', border: 'border-orange-500/40', dot: 'bg-orange-400' },
  { label: 'Kafka Topic',        desc: 'telemetry.raw · 3 partitions',        icon: '📨', gradient: 'from-amber-500/20 to-amber-600/5',  border: 'border-amber-500/40',  dot: 'bg-amber-400' },
  { label: 'PyFlink KafkaSource', desc: 'Consumer group: flink-telemetry',    icon: '⚡', gradient: 'from-sky-500/20 to-sky-600/5',      border: 'border-sky-500/40',    dot: 'bg-sky-400' },
  { label: 'Normalize + Window', desc: 'MinMax → 30-cycle keyed buffer',      icon: '⚙️', gradient: 'from-blue-500/20 to-blue-600/5',    border: 'border-blue-500/40',   dot: 'bg-blue-400' },
  { label: 'Redis Sink',         desc: 'engine:{id}:features · TTL 1h',       icon: '💾', gradient: 'from-green-500/20 to-green-600/5',   border: 'border-green-500/40',  dot: 'bg-green-400' },
  { label: 'FastAPI Inference',   desc: 'GRU → RUL prediction',               icon: '🧠', gradient: 'from-purple-500/20 to-purple-600/5', border: 'border-purple-500/40', dot: 'bg-purple-400' },
]
</script>

<template>
  <DashboardLayout>
    <div class="p-6 space-y-6">
      <!-- Header -->
      <div class="flex items-center justify-between">
        <div>
          <h1 class="text-lg font-semibold text-white">Streaming Pipeline Monitor</h1>
          <p class="text-xs text-gray-500 mt-0.5">Solace → Kafka → Flink → Redis → FastAPI</p>
        </div>
        <div class="flex items-center gap-4">
          <!-- Flink job badge -->
          <div v-if="flinkJobs.length" class="flex items-center gap-2 bg-sky-500/10 border border-sky-500/30 rounded-full px-3 py-1">
            <span class="w-2 h-2 rounded-full bg-sky-400 animate-pulse"></span>
            <span class="text-xs text-sky-300 font-mono">{{ flinkJobs.length }} Flink job{{ flinkJobs.length > 1 ? 's' : '' }} running</span>
          </div>
          <div v-else class="flex items-center gap-2 bg-gray-800/50 border border-gray-700 rounded-full px-3 py-1">
            <span class="w-2 h-2 rounded-full bg-gray-600"></span>
            <span class="text-xs text-gray-500 font-mono">No Flink jobs</span>
          </div>
          <!-- WS status -->
          <div class="flex items-center gap-2 text-xs">
            <span :class="store.wsConnected ? 'bg-green-400' : 'bg-red-500'" class="w-2 h-2 rounded-full animate-pulse"></span>
            <span class="text-gray-500">{{ store.wsConnected ? 'Live stream' : 'Connecting…' }}</span>
          </div>
        </div>
      </div>

      <!-- Live counters -->
      <div class="grid grid-cols-2 md:grid-cols-4 gap-4">
        <div class="bg-card border border-border rounded-lg p-4 relative overflow-hidden">
          <div class="absolute inset-0 bg-gradient-to-br from-cyan-500/5 to-transparent"></div>
          <p class="text-xs text-gray-500 uppercase tracking-widest mb-1 relative">Active Engines</p>
          <p class="text-2xl font-semibold text-white relative">{{ activeEngines }}</p>
          <p class="text-xs text-gray-600 mt-1 relative">with predictions</p>
        </div>
        <div class="bg-card border border-border rounded-lg p-4 relative overflow-hidden">
          <div class="absolute inset-0 bg-gradient-to-br from-blue-500/5 to-transparent"></div>
          <p class="text-xs text-gray-500 uppercase tracking-widest mb-1 relative">Telemetry Engines</p>
          <p class="text-2xl font-semibold text-cyan-400 relative">{{ telemetryCount }}</p>
          <p class="text-xs text-gray-600 mt-1 relative">in Redis feature store</p>
        </div>
        <div class="bg-card border border-border rounded-lg p-4 relative overflow-hidden">
          <div class="absolute inset-0 bg-gradient-to-br from-indigo-500/5 to-transparent"></div>
          <p class="text-xs text-gray-500 uppercase tracking-widest mb-1 relative">Avg Cycle</p>
          <p class="text-2xl font-semibold text-white relative">{{ avgCycle || '—' }}</p>
          <p class="text-xs text-gray-600 mt-1 relative">across fleet</p>
        </div>
        <div class="bg-card border border-border rounded-lg p-4 relative overflow-hidden">
          <div class="absolute inset-0 bg-gradient-to-br from-red-500/5 to-transparent"></div>
          <p class="text-xs text-gray-500 uppercase tracking-widest mb-1 relative">Critical</p>
          <p :class="criticalCount > 0 ? 'text-red-400' : 'text-white'" class="text-2xl font-semibold relative">
            {{ criticalCount }}
          </p>
          <p class="text-xs text-gray-600 mt-1 relative">engines at risk</p>
        </div>
      </div>

      <!-- Pipeline topology -->
      <div class="bg-card border border-border rounded-lg p-5">
        <div class="flex items-center justify-between mb-5">
          <h3 class="text-sm font-semibold text-gray-300">Pipeline Topology</h3>
          <span class="text-xs text-gray-600">8-stage streaming architecture</span>
        </div>

        <!-- Desktop: horizontal flow -->
        <div class="hidden lg:block">
          <div class="flex items-center gap-0">
            <div v-for="(step, i) in pipeline" :key="step.label" class="flex items-center">
              <!-- Stage card -->
              <div :class="`bg-gradient-to-br ${step.gradient} border ${step.border} rounded-xl px-4 py-3 min-w-[140px] relative group hover:scale-105 transition-transform duration-200`">
                <div class="flex items-center gap-2 mb-1.5">
                  <span :class="`w-2 h-2 rounded-full ${step.dot} shadow-lg`"></span>
                  <span class="text-[11px] text-white font-semibold tracking-wide">{{ step.label }}</span>
                </div>
                <p class="text-[10px] text-gray-500 leading-relaxed">{{ step.desc }}</p>
                <span class="text-base absolute -top-2 -right-2 opacity-70 group-hover:opacity-100 transition-opacity">{{ step.icon }}</span>
              </div>
              <!-- Animated arrow -->
              <div v-if="i < pipeline.length - 1" class="flex items-center px-1 shrink-0">
                <div class="pipeline-arrow">
                  <svg width="32" height="16" viewBox="0 0 32 16" fill="none">
                    <line x1="0" y1="8" x2="24" y2="8" stroke="url(#arrowGrad)" stroke-width="2" stroke-dasharray="4 3" class="animate-dash" />
                    <polygon points="22,4 30,8 22,12" fill="#475569" class="animate-pulse" />
                    <defs>
                      <linearGradient id="arrowGrad" x1="0" y1="0" x2="32" y2="0">
                        <stop offset="0%" stop-color="#334155" />
                        <stop offset="100%" stop-color="#64748b" />
                      </linearGradient>
                    </defs>
                  </svg>
                </div>
              </div>
            </div>
          </div>
        </div>

        <!-- Mobile: vertical flow -->
        <div class="lg:hidden space-y-2">
          <div v-for="(step, i) in pipeline" :key="step.label">
            <div :class="`bg-gradient-to-br ${step.gradient} border ${step.border} rounded-xl px-4 py-3 relative`">
              <div class="flex items-center gap-2 mb-1">
                <span :class="`w-2 h-2 rounded-full ${step.dot}`"></span>
                <span class="text-xs text-white font-semibold">{{ step.icon }} {{ step.label }}</span>
              </div>
              <p class="text-[10px] text-gray-500 pl-4">{{ step.desc }}</p>
            </div>
            <div v-if="i < pipeline.length - 1" class="flex justify-center py-1 text-gray-600">
              <svg width="16" height="20" viewBox="0 0 16 20"><line x1="8" y1="0" x2="8" y2="14" stroke="#475569" stroke-width="2" stroke-dasharray="3 2" /><polygon points="4,12 12,12 8,20" fill="#475569" /></svg>
            </div>
          </div>
        </div>
      </div>

      <!-- Live telemetry table -->
      <div class="bg-card border border-border rounded-lg p-4">
        <div class="flex items-center justify-between mb-3">
          <h3 class="text-sm font-semibold text-gray-300">Live Telemetry Feed</h3>
          <span class="text-xs text-gray-600">{{ store.lastUpdated ? `Updated ${store.lastUpdated}` : 'Waiting for stream…' }}</span>
        </div>
        <div v-if="!telemetryCount" class="text-xs text-gray-600 py-4 text-center">
          No telemetry yet — start the producer and consumer.
        </div>
        <div v-else class="overflow-auto max-h-52">
          <table class="w-full text-xs">
            <thead>
              <tr class="text-gray-500 border-b border-border">
                <th class="text-left py-2 pr-4">Engine</th>
                <th class="text-right pr-4">Cycle</th>
                <th class="text-right pr-4">Window</th>
                <th class="text-right">Last Event</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="[id, meta] in [...store.telemetry.entries()].slice(0, 20)"
                :key="id" class="border-b border-border/40 hover:bg-white/[0.02] transition-colors">
                <td class="py-1.5 pr-4 font-mono text-cyan-400">{{ id }}</td>
                <td class="text-right pr-4 text-white font-mono">{{ meta.cycle }}</td>
                <td class="text-right pr-4 text-gray-400">{{ meta.window_size }}</td>
                <td class="text-right text-gray-600 font-mono">
                  {{ meta.event_time_ms ? new Date(meta.event_time_ms).toLocaleTimeString() : '—' }}
                </td>
              </tr>
            </tbody>
          </table>
          <p v-if="telemetryCount > 20" class="text-xs text-gray-600 mt-2 text-center">
            Showing 20 of {{ telemetryCount }} engines
          </p>
        </div>
      </div>

      <!-- Service status -->
      <div class="bg-card border border-border rounded-lg p-4">
        <h3 class="text-sm font-semibold text-gray-300 mb-3">Service Health</h3>
        <div class="grid grid-cols-2 md:grid-cols-4 gap-3">
          <div v-for="svc in services" :key="svc.name"
            class="bg-bg border border-border rounded-lg p-3 flex items-center justify-between hover:border-gray-600 transition-colors">
            <div>
              <p :class="`text-xs font-semibold ${svc.color}`">{{ svc.icon }} {{ svc.name }}</p>
              <p class="text-xs text-gray-600 mt-0.5">:{{ svc.port }}</p>
            </div>
            <div class="flex items-center gap-2">
              <span v-if="svc.status === 'checking'" class="w-2 h-2 rounded-full bg-gray-500 animate-pulse"></span>
              <span v-else-if="svc.status === 'up'"  class="w-2 h-2 rounded-full bg-green-400 shadow-[0_0_6px_rgba(74,222,128,0.5)]"></span>
              <span v-else                            class="w-2 h-2 rounded-full bg-red-500 shadow-[0_0_6px_rgba(239,68,68,0.5)]"></span>
              <a v-if="svc.url" :href="svc.url" target="_blank"
                class="text-xs text-gray-600 hover:text-accent">↗</a>
            </div>
          </div>
        </div>
      </div>

      <!-- Quick commands -->
      <div class="bg-card border border-border rounded-lg p-4">
        <h3 class="text-sm font-semibold text-gray-300 mb-3">Quick Start</h3>
        <div class="space-y-2 text-xs font-mono">
          <div class="bg-bg rounded p-2.5 text-gray-400 flex items-center gap-2">
            <span class="text-green-500 select-none">$</span>
            <span>docker compose --profile streaming up -d</span>
          </div>
          <div class="bg-bg rounded p-2.5 text-gray-400 flex items-center gap-2">
            <span class="text-green-500 select-none">$</span>
            <span>docker compose --profile monitoring up -d</span>
          </div>
          <div class="bg-bg rounded p-2.5 text-gray-400 flex items-center gap-2">
            <span class="text-green-500 select-none">$</span>
            <span>curl http://localhost:8082/jobs  <span class="text-gray-600"># Check Flink jobs</span></span>
          </div>
        </div>
      </div>
    </div>
  </DashboardLayout>
</template>

<style scoped>
@keyframes dash {
  to { stroke-dashoffset: -14; }
}
.animate-dash {
  animation: dash 1s linear infinite;
}
</style>
