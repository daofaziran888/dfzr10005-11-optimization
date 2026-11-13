<template>
  <section class="page" data-module="paths">
    <header class="page-head">
      <div>
        <h2>路径计算中心</h2>
        <p class="page-desc">基线与 SLA 共用同一份路径计算结果。集中呈现路径版本、承诺窗口、增量批次与可追溯视图，节点状态变化后不沿用旧摘要。</p>
      </div>
      <div class="page-actions">
        <button class="btn" type="button" @click="reload">刷新快照</button>
      </div>
    </header>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">最近计算水位</span>
        <strong class="stat-value">{{ center?.watermark ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">当前口径版本</span>
        <strong class="stat-value">v{{ center?.caliber_version ?? 1 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">路径数量</span>
        <strong class="stat-value">{{ center?.paths.length ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">事件批次</span>
        <strong class="stat-value">{{ center?.batches.length ?? 0 }}</strong>
      </article>
    </div>

    <h3 class="block-title">当前路径摘要（最新版本快照）</h3>
    <table class="data-table">
      <thead>
        <tr>
          <th>路径</th><th>版本</th><th>口径</th><th>水位</th><th>超时预算(分钟)</th>
          <th>上一版预算</th><th>承诺时间</th><th>预警时间</th><th>末端最近产出</th>
          <th>上游顺序</th><th>下游集合</th><th>风险等级</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="path in center?.paths ?? []" :key="path.path_id">
          <td>{{ path.path_id }}</td>
          <td>{{ path.path_version }}</td>
          <td>v{{ path.caliber_version }}</td>
          <td>{{ path.watermark }}</td>
          <td>{{ path.timeout_budget_minutes }}</td>
          <td>{{ path.previous_timeout_budget_minutes ?? '—' }}</td>
          <td>{{ path.promise_time ?? '—' }}</td>
          <td>{{ path.warn_time ?? '—' }}</td>
          <td>{{ path.sink_latest_output ?? '—' }}</td>
          <td>{{ path.upstream_order.join(' → ') || '—' }}</td>
          <td>{{ path.downstream_set.join('、') || '—' }}</td>
          <td><span :class="['risk-badge', riskClass(riskOf(path.path_id))]">{{ riskOf(path.path_id) }}</span></td>
        </tr>
      </tbody>
    </table>

    <div class="two-col">
      <section class="trace-panel">
        <h3 class="block-title">已完成节点视图</h3>
        <table class="data-table compact">
          <thead><tr><th>节点编号</th><th>状态</th><th>超时分钟</th><th>最近产出</th></tr></thead>
          <tbody>
            <tr v-for="node in center?.traceability.completed_nodes_view ?? []" :key="node.节点编号">
              <td>{{ node.节点编号 }}</td><td>{{ node.节点状态 }}</td><td>{{ node.超时分钟 }}</td><td>{{ node.最近产出 ?? '—' }}</td>
            </tr>
          </tbody>
        </table>
      </section>

      <section class="trace-panel">
        <h3 class="block-title">当前 SLA 风险视图</h3>
        <table class="data-table compact">
          <thead><tr><th>路径</th><th>版本</th><th>风险等级</th><th>承诺时间</th></tr></thead>
          <tbody>
            <tr v-for="risk in center?.traceability.current_sla_risk_view ?? []" :key="risk.path_id">
              <td>{{ risk.path_id }}</td>
              <td>{{ risk.path_version }}</td>
              <td><span :class="['risk-badge', riskClass(risk.风险等级)]">{{ risk.风险等级 }}</span></td>
              <td>{{ risk.promise_time ?? '—' }}</td>
            </tr>
          </tbody>
        </table>
      </section>
    </div>

    <section class="trace-panel">
      <h3 class="block-title">历史基线视图 · 路径版本与承诺窗口对应关系</h3>
      <div v-for="item in center?.traceability.historical_baseline_view ?? []" :key="item.path_id" class="version-block">
        <strong class="version-path">{{ item.path_id }}</strong>
        <table class="data-table compact">
          <thead><tr><th>版本</th><th>口径</th><th>水位</th><th>承诺时间</th><th>预警时间</th><th>超时预算</th><th>来源批次</th></tr></thead>
          <tbody>
            <tr v-for="version in item.versions" :key="version.path_version">
              <td>{{ version.path_version }}</td>
              <td>v{{ version.caliber_version }}</td>
              <td>{{ version.watermark }}</td>
              <td>{{ version.promise_time ?? '—' }}</td>
              <td>{{ version.warn_time ?? '—' }}</td>
              <td>{{ version.timeout_budget_minutes }}</td>
              <td>{{ version.batch_id }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>

    <section class="trace-panel">
      <h3 class="block-title">状态事件批次与增量记录</h3>
      <table class="data-table compact">
        <thead>
          <tr><th>批次</th><th>状态</th><th>水位</th><th>事件</th><th>受影响路径</th><th>复用路径</th><th>新版本</th><th>断点</th></tr>
        </thead>
        <tbody>
          <tr v-for="batch in center?.batches ?? []" :key="batch.batch_id">
            <td>{{ batch.batch_id }}</td>
            <td><span :class="['batch-badge', batch.status === 'failed' ? 'batch-failed' : 'batch-ok']">{{ batch.status }}</span></td>
            <td>{{ batch.watermark }}</td>
            <td>{{ formatEvents(batch.events) }}</td>
            <td>{{ batch.affected_paths.join('、') || '—' }}</td>
            <td>{{ batch.reused_paths.join('、') || '—' }}</td>
            <td>{{ batch.new_versions.join('、') || '—' }}</td>
            <td>
              <template v-if="batch.checkpoints.length">
                <div v-for="cp in batch.checkpoints" :key="cp.path_id" class="checkpoint">
                  {{ cp.path_id }}：已完成 {{ cp.done_nodes.join('→') || '—' }} ；剩余 {{ cp.remaining_nodes.join('→') }}
                </div>
              </template>
              <span v-else>—</span>
            </td>
          </tr>
        </tbody>
      </table>
    </section>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import { fetchJson } from '@/api/client'

interface PathSummary {
  path_id: string
  path_version: string
  caliber_version: number
  watermark: number
  timeout_budget_minutes: number
  previous_timeout_budget_minutes: number | null
  promise_time: string | null
  warn_time: string | null
  sink_latest_output: string | null
  upstream_order: string[]
  downstream_set: string[]
}

interface CompletedNode {
  节点编号: string
  节点状态: string
  超时分钟: number
  最近产出: string | null
}

interface SlaRisk {
  path_id: string
  path_version: string
  风险等级: string
  promise_time: string | null
}

interface BaselineVersion {
  path_version: string
  caliber_version: number
  watermark: number
  promise_time: string | null
  warn_time: string | null
  timeout_budget_minutes: number
  batch_id: string
}

interface HistoricalBaseline {
  path_id: string
  versions: BaselineVersion[]
}

interface Checkpoint {
  path_id: string
  done_nodes: string[]
  remaining_nodes: string[]
  error: string
}

interface PathEvent {
  node: string
  status: string
}

interface BatchRecord {
  batch_id: string
  status: string
  watermark: number
  events: PathEvent[]
  affected_paths: string[]
  reused_paths: string[]
  new_versions: string[]
  checkpoints: Checkpoint[]
}

interface PathCenter {
  watermark: number
  caliber_version: number
  paths: PathSummary[]
  batches: BatchRecord[]
  traceability: {
    completed_nodes_view: CompletedNode[]
    historical_baseline_view: HistoricalBaseline[]
    current_sla_risk_view: SlaRisk[]
  }
}

const center = ref<PathCenter | null>(null)

function riskClass(level: string): string {
  if (level === '高风险') return 'risk-high'
  if (level === '中风险') return 'risk-mid'
  if (level === '已完成') return 'risk-done'
  return 'risk-low'
}

function riskOf(pathId: string): string {
  const found = center.value?.traceability.current_sla_risk_view.find((item) => item.path_id === pathId)
  return found?.风险等级 ?? '无承诺窗口'
}

function formatEvents(events: PathEvent[]): string {
  return events.map((event) => `${event.node}→${event.status}`).join('，')
}

async function reload() {
  center.value = await fetchJson<PathCenter>('/api/paths/center')
}

onMounted(reload)
</script>

<style scoped>
.block-title { margin: 18px 0 8px; font-size: 15px; }
.trace-panel { margin-bottom: 16px; border: 1px solid var(--border); border-radius: 8px; padding: 10px 12px; background: #fff; }
.two-col { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
.compact th, .compact td { padding: 6px 8px; font-size: 12px; }
.risk-badge, .batch-badge { display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 12px; font-weight: 600; }
.risk-high, .batch-failed { background: #fee4e2; color: #b42318; }
.risk-mid { background: #fef0c7; color: #b54708; }
.risk-low, .batch-ok { background: #dcfae6; color: #067647; }
.risk-done { background: #e0eaff; color: #1849a9; }
.version-block { margin-bottom: 12px; }
.version-path { font-size: 13px; color: var(--brand); }
.checkpoint { font-size: 12px; color: #b42318; }
</style>
