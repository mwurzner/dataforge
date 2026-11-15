/* Laadbarometer: turns latest.json into the page. Pure functions first, so they can be tested
   without a browser; the last block wires them to the document. */
(function (root) {
  'use strict';

  var nf0 = new Intl.NumberFormat('nl-NL', { maximumFractionDigits: 0 });
  var nf1 = new Intl.NumberFormat('nl-NL', { minimumFractionDigits: 1, maximumFractionDigits: 1 });
  var nf2 = new Intl.NumberFormat('nl-NL', { minimumFractionDigits: 2, maximumFractionDigits: 2 });

  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }
  function pct(part, whole) { return whole ? 100 * part / whole : 0; }
  function euro(v) { return v == null ? '–' : '€ ' + nf2.format(v); }
  function when(iso) {
    var d = new Date(iso);
    if (isNaN(d)) { return esc(iso); }
    return d.toLocaleString('nl-NL', { timeZone: 'Europe/Amsterdam', dateStyle: 'long', timeStyle: 'short' });
  }

  /* One row per operator, with the seven-day average where enough snapshots exist. */
  function operatorRows(d) {
    var avg = (d.average && d.average.operators) || {};
    return d.operators.map(function (o) {
      var a = avg[o.name];
      return {
        name: o.name, points: o.points, averaged: !!a,
        storing: a ? a.outoforder : pct(o.outoforder, o.points),
        onbekend: a ? a.unknown : pct(o.unknown, o.points),
        nietInBedrijf: a ? a.inoperative : pct(o.inoperative, o.points),
        laden: a ? a.charging : pct(o.charging, o.points),
        prijs: o.price_ac
      };
    });
  }

  function bar(value, max, cls) {
    var w = Math.max(0, Math.min(100, max ? 100 * value / max : 0));
    return '<span class="bar ' + cls + '"><span style="width:' + w.toFixed(1) + '%"></span></span>';
  }

  function tiles(d) {
    var n = d.national, a = d.average && d.average.national;
    var items = [
      ['Publieke laadpunten', nf0.format(n.points), 'in de landelijke open data'],
      ['Nu aan het laden', nf1.format(pct(n.charging, n.points)) + '%', nf0.format(n.charging) + ' laadpunten bezet'],
      ['In storing', nf1.format(a ? a.outoforder : pct(n.outoforder, n.points)) + '%',
        a ? 'gemiddeld over 7 dagen' : nf0.format(n.outoforder) + ' laadpunten, momentopname'],
      ['Status onbekend', nf1.format(a ? a.unknown : pct(n.unknown, n.points)) + '%',
        'geven geen bruikbare status door'],
      ['Gewoon laden (AC)', d.prices.ac ? euro(d.prices.ac.median) : '–',
        d.prices.ac ? 'mediaan per kWh; 8 op de 10 tussen ' + euro(d.prices.ac.p10) + ' en ' + euro(d.prices.ac.p90) : ''],
      ['Snelladen (DC)', d.prices.dc ? euro(d.prices.dc.median) : '–', 'mediaan per kWh']
    ];
    return items.map(function (t) {
      return '<div class="tile"><div class="label">' + esc(t[0]) + '</div><div class="value">' + esc(t[1]) +
        '</div><div class="sub">' + esc(t[2]) + '</div></div>';
    }).join('');
  }

  function operatorTable(rows, sortKey, desc) {
    var sorted = rows.slice().sort(function (x, y) {
      var a = x[sortKey], b = y[sortKey];
      if (a == null) { return 1; }
      if (b == null) { return -1; }
      if (typeof a === 'string') { return desc ? b.localeCompare(a, 'nl') : a.localeCompare(b, 'nl'); }
      return desc ? b - a : a - b;
    });
    var maxStoring = Math.max.apply(null, rows.map(function (r) { return r.storing || 0; }).concat([1]));
    var maxOnbekend = Math.max.apply(null, rows.map(function (r) { return r.onbekend || 0; }).concat([1]));
    var head = [['name', 'Exploitant'], ['points', 'Laadpunten'], ['storing', 'In storing'], ['onbekend', 'Status onbekend'],
      ['nietInBedrijf', 'Niet in bedrijf'], ['laden', 'Aan het laden'], ['prijs', 'Prijs per kWh (AC)']];
    var h = '<thead><tr>' + head.map(function (c) {
      var mark = c[0] === sortKey ? (desc ? ' ▼' : ' ▲') : '';
      return '<th data-sort="' + c[0] + '"' + (c[0] === 'name' ? '' : ' class="num"') + '>' + esc(c[1]) + mark + '</th>';
    }).join('') + '</tr></thead>';
    var b = sorted.map(function (r) {
      return '<tr><td>' + esc(r.name) + '</td><td class="num">' + nf0.format(r.points) + '</td>' +
        '<td class="num">' + bar(r.storing, maxStoring, 'bad') + nf1.format(r.storing) + '%</td>' +
        '<td class="num">' + bar(r.onbekend, maxOnbekend, 'warn') + nf1.format(r.onbekend) + '%</td>' +
        '<td class="num">' + nf1.format(r.nietInBedrijf) + '%</td>' +
        '<td class="num">' + nf1.format(r.laden) + '%</td>' +
        '<td class="num">' + euro(r.prijs) + '</td></tr>';
    }).join('');
    return h + '<tbody>' + b + '</tbody>';
  }

  function cityList(list) {
    return '<ol>' + list.map(function (c) {
      return '<li><span>' + esc(c.city) + '</span><span class="num">' + euro(c.median) +
        ' <small>' + nf0.format(c.n) + ' laadpunten</small></span></li>';
    }).join('') + '</ol>';
  }

  function basis(d) {
    var a = d.average;
    if (a) {
      return 'Storing, status en bezetting zijn het gemiddelde van ' + nf0.format(a.snapshots) +
        ' momentopnamen tussen ' + when(a.from) + ' en ' + when(a.to) + '.';
    }
    return 'Dit is één momentopname. Zodra er genoeg metingen zijn, tonen we het gemiddelde over zeven dagen; dat is eerlijker dan één moment.';
  }

  var api = { esc: esc, pct: pct, euro: euro, when: when, operatorRows: operatorRows, tiles: tiles,
    operatorTable: operatorTable, cityList: cityList, basis: basis };
  root.Laadbarometer = api;
  if (typeof module !== 'undefined' && module.exports) { module.exports = api; }

  if (typeof document === 'undefined') { return; }

  function $(id) { return document.getElementById(id); }

  function show(d) {
    var rows = operatorRows(d), sortKey = 'points', desc = true;
    $('updated').textContent = 'Bijgewerkt: ' + when(d.generated);
    $('tiles').innerHTML = tiles(d);
    $('basis').textContent = basis(d);
    function draw() { $('operators').innerHTML = operatorTable(rows, sortKey, desc); }
    $('operators').addEventListener('click', function (e) {
      var th = e.target.closest('th');
      if (!th) { return; }
      var k = th.getAttribute('data-sort');
      desc = k === sortKey ? !desc : k !== 'name';
      sortKey = k;
      draw();
    });
    draw();
    $('cheapest').innerHTML = cityList(d.cities.cheapest);
    $('dearest').innerHTML = cityList(d.cities.dearest);
    $('cities-note').textContent = 'Plaatsnamen zoals exploitanten ze aanleveren. Alleen plaatsen met minstens 300 laadpunten met een bekende prijs (' +
      nf0.format(d.cities.counted) + ' plaatsen).';
    document.body.classList.add('ready');
  }

  fetch('latest.json?t=' + Date.now(), { cache: 'no-store' })
    .then(function (r) { if (!r.ok) { throw new Error('HTTP ' + r.status); } return r.json(); })
    .then(show)
    .catch(function (err) {
      $('updated').textContent = 'De cijfers konden niet worden geladen (' + err.message + '). Probeer het later opnieuw.';
    });
})(typeof globalThis !== 'undefined' ? globalThis : this);
