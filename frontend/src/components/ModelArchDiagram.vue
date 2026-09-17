<script setup lang="ts">
const layers = [
  { type: 'input',   label: 'INPUT',  sub: '30×11',   color: '#00d9ff', grad: '#0891b2', width: 72,  height: 130, units: '330' },
  { type: 'gru',     label: 'GRU',    sub: '128',     color: '#3b82f6', grad: '#1d4ed8', width: 58,  height: 175, units: '128' },
  { type: 'dropout', label: 'DROP',   sub: '0.2',     color: '#a855f7', grad: '#7c3aed', width: 26,  height: 148, units: '' },
  { type: 'gru',     label: 'GRU',    sub: '64',      color: '#2563eb', grad: '#1e40af', width: 52,  height: 140, units: '64' },
  { type: 'dropout', label: 'DROP',   sub: '0.2',     color: '#a855f7', grad: '#7c3aed', width: 26,  height: 120, units: '' },
  { type: 'gru',     label: 'GRU',    sub: '32',      color: '#1d4ed8', grad: '#1e3a8a', width: 44,  height: 105, units: '32' },
  { type: 'dropout', label: 'DROP',   sub: '0.15',    color: '#9333ea', grad: '#6b21a8', width: 24,  height: 85,  units: '' },
  { type: 'dense',   label: 'DENSE',  sub: '32',      color: '#f59e0b', grad: '#d97706', width: 36,  height: 76,  units: '32' },
  { type: 'dense',   label: 'DENSE',  sub: '16',      color: '#eab308', grad: '#ca8a04', width: 30,  height: 58,  units: '16' },
  { type: 'output',  label: 'RUL',    sub: '×125',    color: '#22c55e', grad: '#16a34a', width: 20,  height: 38,  units: '1' },
]

const spacing = 92
const startX  = 60
const centerY = 148
const svgW    = startX + layers.length * spacing + 40
const svgH    = 280

function getX(i: number) { return startX + i * spacing }
</script>

<template>
  <div class="w-full overflow-x-auto rounded-xl border border-[#1e293b] bg-[#060e1f] p-5 relative">
    <!-- Subtle background grid -->
    <div class="absolute inset-0 opacity-[0.03] pointer-events-none"
         style="background-image: radial-gradient(circle, #3b82f6 1px, transparent 1px); background-size: 24px 24px;"></div>

    <svg :width="svgW" :height="svgH" class="min-w-[1050px] relative">
      <defs>
        <!-- Glow filters -->
        <filter id="glow-soft">
          <feGaussianBlur stdDeviation="3" result="blur" />
          <feMerge><feMergeNode in="blur" /><feMergeNode in="SourceGraphic" /></feMerge>
        </filter>
        <filter id="glow-strong">
          <feGaussianBlur stdDeviation="6" result="blur" />
          <feMerge><feMergeNode in="blur" /><feMergeNode in="SourceGraphic" /></feMerge>
        </filter>

        <!-- Gradient fills for each layer -->
        <linearGradient v-for="(layer, i) in layers" :key="'grad-' + i"
          :id="'lg-' + i" x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" :stop-color="layer.color" stop-opacity="0.25" />
          <stop offset="100%" :stop-color="layer.grad" stop-opacity="0.08" />
        </linearGradient>

        <!-- Arrow marker -->
        <marker id="flow-arrow" markerWidth="8" markerHeight="8" refX="6" refY="4" orient="auto">
          <path d="M0,1 L0,7 L7,4 z" fill="#475569" opacity="0.8" />
        </marker>

        <!-- Animated data particle -->
        <circle id="data-particle" r="2.5" fill="#00d9ff" opacity="0.9" filter="url(#glow-soft)">
          <animate attributeName="opacity" values="0.9;0.4;0.9" dur="1.5s" repeatCount="indefinite" />
        </circle>
      </defs>

      <!-- ═══ CONNECTIONS WITH ANIMATED PARTICLES ═══ -->
      <g v-for="(layer, i) in layers.slice(0, -1)" :key="'conn-' + i">
        <!-- Base connection line -->
        <line
          :x1="getX(i) + layer.width + 8"
          :y1="centerY"
          :x2="getX(i + 1) - 6"
          :y2="centerY"
          stroke="#1e293b" stroke-width="2"
        />
        <!-- Animated dashed overlay -->
        <line
          :x1="getX(i) + layer.width + 8"
          :y1="centerY"
          :x2="getX(i + 1) - 6"
          :y2="centerY"
          stroke-width="1.5"
          stroke-dasharray="6 4"
          class="animate-dash"
          :stroke="layer.color"
          opacity="0.4"
        />
        <!-- Flowing data particle -->
        <circle r="3" :fill="layer.color" filter="url(#glow-soft)">
          <animateMotion
            :dur="(1.2 + i * 0.15) + 's'"
            repeatCount="indefinite">
            <mpath>
              <line :x1="getX(i) + layer.width + 8" :y1="centerY"
                    :x2="getX(i + 1) - 6" :y2="centerY" />
            </mpath>
          </animateMotion>
        </circle>
        <!-- Second particle (offset) -->
        <circle r="2" :fill="layers[i+1].color" opacity="0.6" filter="url(#glow-soft)">
          <animateMotion
            :dur="(1.6 + i * 0.1) + 's'"
            repeatCount="indefinite"
            :begin="(0.7 + i * 0.05) + 's'">
            <mpath>
              <line :x1="getX(i) + layer.width + 8" :y1="centerY"
                    :x2="getX(i + 1) - 6" :y2="centerY" />
            </mpath>
          </animateMotion>
        </circle>
      </g>

      <!-- ═══ LAYER BLOCKS ═══ -->
      <g v-for="(layer, i) in layers" :key="'layer-' + i">

        <!-- Ambient glow behind block -->
        <rect
          :x="getX(i) - 4"
          :y="centerY - layer.height / 2 - 4"
          :width="layer.width + 8"
          :height="layer.height + 8"
          :fill="layer.color"
          opacity="0.06"
          rx="10"
          filter="url(#glow-strong)"
        />

        <!-- Main block -->
        <rect
          :x="getX(i)"
          :y="centerY - layer.height / 2"
          :width="layer.width"
          :height="layer.height"
          :fill="`url(#lg-${i})`"
          :stroke="layer.color"
          stroke-width="1.5"
          rx="8"
        />

        <!-- Internal neuron lines for GRU/Dense/Input -->
        <g v-if="layer.type !== 'dropout' && layer.type !== 'output'" opacity="0.3">
          <line
            v-for="n in 5"
            :key="n"
            :x1="getX(i) + 8"
            :x2="getX(i) + layer.width - 8"
            :y1="centerY - layer.height / 2 + n * (layer.height / 6)"
            :y2="centerY - layer.height / 2 + n * (layer.height / 6)"
            :stroke="layer.color"
            stroke-dasharray="2 3"
          />
        </g>

        <!-- Dropout scatter dots -->
        <g v-if="layer.type === 'dropout'" opacity="0.5">
          <circle v-for="n in 6" :key="n"
            :cx="getX(i) + layer.width / 2 + (n % 2 === 0 ? 3 : -3)"
            :cy="centerY - layer.height / 2 + 12 + n * (layer.height - 24) / 6"
            r="2"
            :fill="layer.color"
          >
            <animate attributeName="opacity" values="0.2;0.7;0.2"
              :dur="(0.8 + n * 0.2) + 's'" repeatCount="indefinite" />
          </circle>
        </g>

        <!-- Output neuron -->
        <g v-if="layer.type === 'output'">
          <circle
            :cx="getX(i) + layer.width / 2"
            :cy="centerY"
            r="8"
            :fill="layer.color"
            opacity="0.3"
            filter="url(#glow-soft)"
          >
            <animate attributeName="r" values="7;9;7" dur="2s" repeatCount="indefinite" />
          </circle>
          <circle
            :cx="getX(i) + layer.width / 2"
            :cy="centerY"
            r="5"
            :fill="layer.color"
          />
        </g>

        <!-- Vertical label -->
        <g :transform="`translate(${getX(i) + layer.width / 2}, ${centerY}) rotate(-90)`">
          <text text-anchor="middle" fill="#e2e8f0" font-size="10" font-weight="800" letter-spacing="1.5">
            {{ layer.label }}
          </text>
        </g>

        <!-- Units/sub label below -->
        <text
          :x="getX(i) + layer.width / 2"
          :y="centerY + layer.height / 2 + 16"
          text-anchor="middle"
          :fill="layer.color"
          font-size="10"
          font-weight="600"
          opacity="0.8"
        >
          {{ layer.sub }}
        </text>

        <!-- Top type badge -->
        <g v-if="layer.type === 'gru'">
          <rect
            :x="getX(i) + layer.width / 2 - 14"
            :y="centerY - layer.height / 2 - 12"
            width="28" height="14" rx="4"
            :fill="layer.color" opacity="0.15"
            :stroke="layer.color" stroke-width="0.5"
          />
          <text
            :x="getX(i) + layer.width / 2"
            :y="centerY - layer.height / 2 - 3"
            text-anchor="middle"
            :fill="layer.color"
            font-size="7"
            font-weight="700"
            letter-spacing="0.5"
          >RNN</text>
        </g>
      </g>

      <!-- ═══ TOP ANNOTATIONS ═══ -->
      <text :x="getX(0) + layers[0].width / 2" y="22" text-anchor="middle"
        fill="#64748b" font-size="9" font-weight="600" letter-spacing="1">
        SEQUENCE INPUT
      </text>
      <text :x="(getX(1) + getX(5)) / 2 + 20" y="22" text-anchor="middle"
        fill="#64748b" font-size="9" font-weight="600" letter-spacing="1">
        RECURRENT FEATURE EXTRACTION
      </text>
      <text :x="(getX(7) + getX(9)) / 2 + 10" y="22" text-anchor="middle"
        fill="#64748b" font-size="9" font-weight="600" letter-spacing="1">
        REGRESSION HEAD
      </text>

      <!-- Bracket lines -->
      <line :x1="getX(1)" y1="28" :x2="getX(6) + layers[6].width" y2="28" stroke="#334155" stroke-width="0.5" />
      <line :x1="getX(7)" y1="28" :x2="getX(9) + layers[9].width" y2="28" stroke="#334155" stroke-width="0.5" />

    </svg>

    <!-- Legend -->
    <div class="flex items-center gap-5 mt-3 text-[10px] text-gray-500 pl-2">
      <span class="flex items-center gap-1.5"><span class="w-2.5 h-2.5 rounded bg-blue-500/30 border border-blue-500/50"></span> GRU Layer</span>
      <span class="flex items-center gap-1.5"><span class="w-2.5 h-2.5 rounded bg-purple-500/30 border border-purple-500/50"></span> MC Dropout</span>
      <span class="flex items-center gap-1.5"><span class="w-2.5 h-2.5 rounded bg-amber-500/30 border border-amber-500/50"></span> Dense + ReLU + L2</span>
      <span class="flex items-center gap-1.5"><span class="w-2.5 h-2.5 rounded bg-green-500/30 border border-green-500/50"></span> Sigmoid → RUL</span>
      <span class="flex items-center gap-1.5"><span class="w-2 h-2 rounded-full bg-cyan-400"></span> Data Flow</span>
    </div>
  </div>
</template>

<style scoped>
@keyframes dash {
  to { stroke-dashoffset: -20; }
}
.animate-dash {
  animation: dash 2s linear infinite;
}
</style>
