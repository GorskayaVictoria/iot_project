class CreateSensors < ActiveRecord::Migration[7.0]
  def change
    create_table :sensors do |t|
      t.string :name
      t.string :number
      t.string :value_type
      t.string :value

      t.timestamps
    end
  end
end
