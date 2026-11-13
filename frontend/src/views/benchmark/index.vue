<template>
  <section class="page" data-module="benchmark">
    <header class="page-head">
      <div>
        <h2>查询性能分析管理</h2>
        <p class="page-desc">维护查询性能的执行耗时、索引分析与优化建议，为查询性能分析提供可审计的数据工程流程。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记性能报告</button>
        <button class="btn" type="button" @click="exportRows">导出查询性能分析清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" type="submit">查询</button>
      <button class="btn ghost" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
            <button class="link" type="button" @click="showPathReference(row)">路径摘要引用</button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无查询性能分析数据，可先登记性能报告</td>
        </tr>
      </tbody>
    </table>

    <section v-if="pathReference" class="trace-panel">
      <header class="trace-head">
        <strong>{{ pathReference['报告编号'] }} 冻结引用 {{ pathReference['引用路径'] }} @ {{ pathReference['引用时版本'] }}</strong>
        <button class="btn ghost" type="button" @click="pathReference = null">关闭</button>
      </header>
      <p class="frozen-tip">该副本在引用时定型，后续依赖图节点状态变化不会被重新解释。</p>
      <table class="data-table compact">
        <thead><tr><th>节点编号</th><th>节点状态</th><th>上游顺序</th><th>下游集合</th><th>超时分钟</th><th>最近产出</th><th>超时-产出差值(分钟)</th></tr></thead>
        <tbody>
          <tr v-for="node in pathReference.path_summary.nodes" :key="node['节点编号']">
            <td>{{ node['节点编号'] }}</td>
            <td>{{ node['节点状态'] }}</td>
            <td>{{ node['上游节点顺序'].join('→') || '—' }}</td>
            <td>{{ node['下游节点集合'].join('、') || '—' }}</td>
            <td>{{ node['超时分钟'] }}</td>
            <td>{{ node['最近产出'] ?? '—' }}</td>
            <td>{{ node['超时与最近产出差值分钟'] ?? '—' }}</td>
          </tr>
        </tbody>
      </table>
    </section>

    <footer class="page-foot">
      <span>共 {{ total }} 条查询性能分析记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>

interface PathNode {
  节点编号: string
  节点状态: string
  上游节点顺序: string[]
  下游节点集合: string[]
  超时分钟: number
  最近产出: string | null
  超时与最近产出差值分钟: number | null
}

interface PathReference {
  报告编号: string
  引用路径: string
  引用时版本: string
  frozen_reference: boolean
  path_summary: { nodes: PathNode[] }
}

const ENDPOINT = '/api/benchmark'
const columns = ["报告编号", "查询语句", "执行耗时", "扫描行数", "索引命中", "资源消耗", "优化建议", "分析时间"]
const actions = ["开始分析", "应用优化", "归档报告"]
const statuses = ["草稿", "分析中", "已生成", "已优化"]
const stats = [{"label": "性能报告总数", "value": 0}, {"label": "已生成报告", "value": 0}, {"label": "已优化报告", "value": 0}]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const pathReference = ref<PathReference | null>(null)

function resetFilters() {
  filters.value = {}
  void reload()
}

async function showPathReference(row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row['报告编号']}/path-reference`)
    if (!response.ok) {
      const payload = await response.json().catch(() => ({}))
      throw new Error(payload.detail ?? '路径摘要引用读取失败')
    }
    pathReference.value = await response.json()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '路径摘要引用读取失败'
  }
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function openCreate() {
  errorMessage.value = '性能报告登记入口尚未接入审批流'
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) {
      throw new Error('查询性能分析动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '查询性能分析操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('性能报告列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '查询性能分析列表读取失败'
  }
}

onMounted(reload)
</script>

<style scoped>
.trace-panel { margin-top: 16px; border: 1px solid var(--border); border-radius: 8px; padding: 12px; background: #fff; }
.trace-head { display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px; }
.frozen-tip { font-size: 12px; color: var(--muted); margin: 0 0 8px; }
.compact th, .compact td { padding: 6px 8px; font-size: 12px; }
</style>
