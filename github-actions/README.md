
whylogs can help monitor your ML datasets as part of your GitOps CI/CD pipeline.

The github action used in this example is defined in `.github/workflows/constraints.yml`.
It specifies what actions to take whenever commits are pushed to this repo.

This directory contains whylogs constraints that are applied to a dataset as part of the Github action.
Constraints assert that a logged value or summary statistic is within an expected range.  

```yaml
      - name: expect constraints to fail step
        uses: whylabs/whylogs_action@v1
        id: expect-failure
        with:
          constraintsfile: 'github-actions/constraints-fail.json'
          datafile: 'python/lending_club_1000.csv'
          expect_failure: 'True'
      - name: expect constraints to succeed step
        uses: whylabs/whylogs_action@v1
        id: expect-success
        with:
          constraintsfile: 'github-actions/constraints-succeed.json'
          datafile: 'python/lending_club_1000.csv'
```

We define two steps in our action.  The first runs a set of constraints that are expected to fail.  That 
is done just to check that the constraint logic is working as expected.  The second step applies a set of constraints that are expected to succeed.

## Action Tags

`uses:` references the prepackaged action in the`whylabs/whylogs_action` repo.
That tells github how to run whylogs on parameters you supply.  This is a tag common to all Github actions.

`constraintsfile:` points to a file of constraints defined in this repo.

`datafile:` points to a file containing data to which the constraints should be applied.  Format 
is anything that the pandas package can load, but CSV works well.

`expect_failure:` indicates whether the action is expected to fail or not. Actions are usually 
written to expect success; we include this flag for completeness.

## Constraint Definition

whylogs constraints are specified in JSON. 
Each constraint is bound to a column in the data, and each column may have multiple constraints.
Standard boolean comparison operators are supported -- LT, LE, EQ, NE, GE, GT.
We are actively extending whylogs to support other constraint operators, for example, 
to match regex on strings or to test image features.

Example:
```
{
  "valueConstraints": {
    "loan_amnt": {
      "constraints": [
        {
          "value": 548250.0,
          "op": "LT"
        },
        {
          "value": 2500.0,
          "op": "LT",
          "verbose": true
        }
      ]
    }
  },
  "summaryConstraints": {
    "annual_inc": {
      "constraints": [
        {
          "firstField": "min",
          "value": 0.0,
          "op": "GE"
        }
      ]
    }
  }
}
```

This example shows the definition of two types of constraints; `valueConstraints` and `summaryConstraints`.
Value constraints are applied to every value that is logged for a feature. At a minimum, 
Value constraints must specify a comparison operator and a literal value.
Summary constraints are applied to Whylogs feature summaries. 
They compare fields of the summary to static literals or to another field in the summary,

Constraints may be marked 'verbose' which will log every failure.
```
INFO - value constraint value GT 2500.0 failed on value 2500.0
```
Verbose logging helps identify why a constraint is failing to validate, but can be very chatty if there are a lots of failures.



