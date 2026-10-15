/* agent-db-sim waveform view glue: /signals catalog + /stream SSE → d3-wave.
 *
 * d3-wave consumes WaveGraph-shaped rows: {name, type:{name,width,formatter},
 * data: [[t, value, duration],...], children}. The server pushes one `init`
 * snapshot then `delta` sample rows; we fold them into that shape and hand
 * the result to d3wave.WaveGraph (all locally vendored: d3, fa-icons shim,
 * d3-wave). */
(function () {
    'use strict';

    var stateEl = document.getElementById('stream-state');
    var waveEl = document.getElementById('wave');

    // vendored d3-wave UMD extends the d3 namespace (d3.WaveGraph); older
    // builds referenced it as window.d3wave — accept both.
    var waveGraphFn = function () {
        if (window.d3wave && window.d3wave.WaveGraph) return window.d3wave.WaveGraph;
        if (window.d3 && window.d3.WaveGraph) return window.d3.WaveGraph;
        return null;
    };
    if (!waveGraphFn()) {
        if (stateEl) stateEl.textContent = 'vendor JS missing (d3 / d3-wave)';
        return;
    }

    var signals = {};   // full name -> {type, rows: [{t, value}]}
    var sigNames = [];  // stable ordering

    function note(msg) {
        if (stateEl) stateEl.textContent = msg;
    }

    function ensureSignal(name, type) {
        if (!signals[name]) {
            signals[name] = { type: type || { name: 'wire', width: 1 }, rows: [] };
            sigNames.push(name);
        } else if (type && !signals[name].type.formatter) {
            signals[name].type = type;
        }
        return signals[name];
    }

    function typeFor(entry) {
        if (entry.type === 'string') {
            return {
                name: 'string',
                width: entry.width,
                formatter: function (d) { return String(d); },
            };
        }
        return { name: entry.type, width: entry.width };
    }

    function foldInit(snap) {
        signals = {}; sigNames = [];
        (snap.signals || []).forEach(function (entry) {
            var name = entry.scope ? entry.scope + '.' + entry.name : entry.name;
            ensureSignal(name, typeFor(entry));
        });
        Object.keys(snap.series || {}).forEach(function (name) {
            var sig = ensureSignal(name);   // spill-section signals arrive data-first
            rowsReplace(sig, snap.series[name]);
        });
        renderWave();
        note('init snapshot (' + sigNames.length + ' signals), awaiting deltas…');
    }

    function rowsReplace(sig, pairs) {
        sig.rows = (pairs || []).map(function (x) { return { t: x[0], value: x[1] }; });
    }

    function foldDelta(delta) {
        (delta.new_signals || []).forEach(function (entry) {
            var name = entry.scope ? entry.scope + '.' + entry.name : entry.name;
            ensureSignal(name, typeFor(entry));
        });
        (delta.samples || []).forEach(function (row) {
            var name = row[1];
            var sig = ensureSignal(name);
            var t = row[0], value = row[2];
            var rows = sig.rows;
            var last = rows[rows.length - 1];
            if (!last || t > last.t) rows.push({ t: t, value: value });
        });
        renderWave();
        note('receiving deltas…');
    }

    function renderWave() {
        waveEl.innerHTML = '';
        if (!sigNames.length) { note('no signals yet'); return; }
        var tMax = 0;
        sigNames.forEach(function (n) {
            var rows = signals[n].rows;
            if (rows.length) tMax = Math.max(tMax, rows[rows.length - 1].t);
        });
        // this d3-wave build renders value plateaus through enum rows; the
        // graph is a labelled tree: root 'label' node + one child per signal
        // d3-wave labels ticks assuming VCD timescale quanta (ps base); the
        // parsed stream time is already in sim seconds, so scale by 1e12 to
        // land on the library's "s" unit ladder.
        var TS = 1e12;
        var children = sigNames.map(function (name) {
            var sig = signals[name];
            var rows = sig.rows;
            var data = rows.map(function (r, i) {
                var nextT = (i + 1 < rows.length) ? rows[i + 1].t : Math.max(tMax, 1);
                return [r.t * TS, String(r.value), Math.max((nextT - r.t) * TS, 0)];
            });
            return { name: name, type: { name: 'enum' }, data: data };
        });
        var w = Math.max(waveEl.clientWidth - 8, 320);
        var h = (children.length + 1) * 25 + 56;
        var svg = d3.select(waveEl).append('svg')
            .attr('width', w).attr('height', h)
            .style('width', w + 'px').style('height', h + 'px');
        try {
            var WGF = waveGraphFn();
            var wg = new WGF(svg);
            wg.bindData({ name: 'signals', type: { name: 'label' },
                          children: children });
        } catch (e) {
            note('wave render failed: ' + e);
        }
    }

    function connectStream() {
        var source = new EventSource('/stream');
        source.addEventListener('init', function (ev) {
            try {
                foldInit(JSON.parse(ev.data));
            } catch (e) {
                note('bad init payload');
            }
        });
        source.addEventListener('delta', function (ev) {
            foldDelta(JSON.parse(ev.data));
        });
        source.addEventListener('done', function (ev) {
            try {
                var why = (JSON.parse(ev.data) || {}).reason || 'finished';
                note('run finished (' + why + ') — deep-dive via the Surfer link above');
            } catch (e) { note('done'); }
            source.close();
        });
        source.onerror = function () {
            note('disconnected; the browser reties automatically');
        };
    }

    connectStream();
})();
