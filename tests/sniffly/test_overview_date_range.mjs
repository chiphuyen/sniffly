import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import test from 'node:test';
import vm from 'node:vm';

const source = readFileSync(new URL('../../sniffly/static/js/overview.js', import.meta.url), 'utf8');

for (const days of [30, 90]) {
  test(`30-day summaries do not include the full ${days}-day chart history`, () => {
    const elements = new Map();
    const context = vm.createContext({
      formatNumber: String,
      document: {
        addEventListener() {},
        getElementById(id) {
          if (!elements.has(id)) elements.set(id, { style: {} });
          return elements.get(id);
        },
      },
    });
    vm.runInContext(source, context);
    context.updateGlobalStatsUI({
      first_use_date: '2000-01-01',
      total_cost: 99,
      total_input_tokens: 123,
      total_output_tokens: 234,
      daily_costs: Array.from({ length: days }, () => ({ cost: 3 })),
      daily_token_usage: Array.from({ length: days }, () => ({ input: 10, output: 20 })),
    });
    assert.equal(elements.get('total-cost-30-days').textContent, '30-day: $90.00');
    assert.match(elements.get('token-30-days').innerHTML, />300<\/span> input/);
    assert.match(elements.get('token-30-days').innerHTML, />600<\/span> output/);
    assert.equal(elements.get('total-cost-all-time').textContent, 'All-time: $99.00');
    assert.match(elements.get('token-all-time').innerHTML, />123<\/span> input/);
  });
}

test('charts can select 90 days while their default view stays at 30 days', () => {
  const context = vm.createContext({ document: { addEventListener() {} } });
  vm.runInContext(source, context);
  const rows = Array.from({ length: 90 }, (_, index) => ({
    date: new Date(Date.UTC(2026, 0, index + 1)).toISOString().slice(0, 10),
  }));
  context.history = { daily_token_usage: rows, daily_costs: rows };
  vm.runInContext('fullGlobalStats = history;', context);
  let tokens;
  let costs;
  context.renderTokenUsageChart = data => { tokens = data; };
  context.renderCostTrendChart = data => { costs = data; };
  context.renderTokenUsageChartWithDateRange('2026-01-01', '2026-03-31');
  context.renderCostTrendChartWithDateRange('2026-01-01', '2026-03-31');
  assert.equal(tokens.length, 90);
  assert.equal(costs.length, 90);
  context.renderTokenUsageChartWithDateRange();
  context.renderCostTrendChartWithDateRange();
  assert.equal(tokens.length, 30);
  assert.equal(costs.length, 30);
});
