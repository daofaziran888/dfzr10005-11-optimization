<template>
  <section class="page" data-module="sla">
    <header class="page-head">
      <div>
        <h2>SLA告警管理管理</h2>
        <p class="page-desc">维护SLA告警的产出时间约束、延迟阈值与升级策略，为SLA保障提供可验证的数据工程流程。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记SLA规则</button>
        <button class="btn" type="button" @click="exportRows">导出SLA告警管理清单</button>
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
          <td v-for="column in columns" :key="column">
            <span v-if="column === '风险等级'" :class="['risk-badge', riskClass(row[column])]">{{ row[column] ?? '—' }}</span>
            <span v-else>{{ row[column] ?? '—' }}</span>
          </td>
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
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无SLA告警管理数据，可先登记SLA规则</td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span>共 {{ total }} 条SLA告警记录 · 风险等级与基线结果共用同一份路径摘要</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>

const ENDPOINT = '/api/sla'
const columns = ['规则编号', '监控对象', 'path_version', 'watermark', '产出时间', '承诺时间', '预警时间', '延迟阈值分钟', '风险等级', '告警级别', '承诺窗口对应版本']
const actions = ['启用监控', '处理告警', '关闭规则']

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = ['规则编号', '监控对象']

const stats = computed(() => [
  { label: 'SLA规则总数', value: rows.value.length },
  { label: '监控中规则', value: rows.value.filter((row) => row.status === '监控中').length },
  { label: '高风险规则', value: rows.value.filter((row) => row['风险等级'] === '高风险').length },
])

function riskClass(level: unknown): string {
  if (level === '高风险') return 'risk-high'
  if (level === '中风险') return 'risk-mid'
  if (level === '已完成') return 'risk-done'
  return 'risk-low'
}

function resetFilters() {
  filters.value = {}
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function openCreate() {
  errorMessage.value = 'SLA规则登记入口尚未接入审批流'
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) {
      throw new Error('SLA告警管理动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : 'SLA告警管理操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('SLA规则列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : 'SLA告警管理列表读取失败'
  }
}

onMounted(reload)
</script>

<style scoped>
.risk-badge { display: inline-block; padding: 2px 8px; border-radius: 10px; font-size: 12px; font-weight: 600; }
.risk-high { background: #fee4e2; color: #b42318; }
.risk-mid { background: #fef0c7; color: #b54708; }
.risk-low { background: #dcfae6; color: #067647; }
.risk-done { background: #e0eaff; color: #1849a9; }
</style>
